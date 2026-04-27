"""
Hybrid Collector

Combines account surface queries with block scanning fallback.
Fast fix + correct fix in one system.
"""
import asyncio
import time
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime

from app.models.provenance import FeeSample
from app.services.solana_collector_fixed import get_solana_collector_fixed, CollectionConfig
from app.services.dex_account_surfaces import get_dex_account_surfaces
from app.services.block_scanning_collector import get_block_scanning_collector, BlockScanConfig

@dataclass
class HybridCollectionConfig:
    """Configuration for hybrid collection."""
    chain: str = "solana"
    network: str = "mainnet-beta"
    tx_family: str = "jupiter_swap"
    program_ids: List[str] = None
    time_window_seconds: int = 3600
    sample_limit: int = 10000
    min_samples: int = 100
    
    # Hybrid strategy config
    use_account_surfaces_first: bool = True
    fallback_to_block_scanning: bool = True
    block_scanning_threshold: float = 0.3  # Use block scanning if account surfaces get < 30% of needed samples

class HybridCollector:
    """
    Hybrid collector that tries account surfaces first, then falls back to block scanning.
    
    Strategy:
    1. Try account surface queries (fast)
    2. If insufficient samples, fall back to block scanning (comprehensive)
    3. Combine results for best coverage
    """
    
    def __init__(self):
        self.dex_surfaces = get_dex_account_surfaces()
        self.solana_collector = get_solana_collector_fixed()
        self.block_scanner = get_block_scanning_collector()
    
    async def collect_fee_samples_hybrid(
        self,
        config: HybridCollectionConfig,
        collection_id: str
    ) -> Tuple[List[FeeSample], Dict[str, Any]]:
        """Collect fee samples using hybrid strategy."""
        start_time = time.time()
        metadata = {
            "collection_id": collection_id,
            "config": config.__dict__,
            "started_at": datetime.utcnow().isoformat(),
            "method": "hybrid_collection",
            "stages": {},
            "samples_collected": 0,
            "verification_status": "pending"
        }
        
        try:
            all_samples = []
            
            # Stage 1: Account surface collection (fast)
            if config.use_account_surfaces_first:
                account_samples, account_metadata = await self._collect_from_account_surfaces(config, collection_id)
                metadata["stages"]["account_surfaces"] = account_metadata
                all_samples.extend(account_samples)
                
                # Check if we need block scanning fallback
                current_sample_count = len(all_samples)
                threshold_samples = int(config.sample_limit * config.block_scanning_threshold)
                
                if current_sample_count < threshold_samples and config.fallback_to_block_scanning:
                    print(f"[Hybrid] Account surfaces got {current_sample_count} samples, falling back to block scanning")
                else:
                    print(f"[Hybrid] Account surfaces got {current_sample_count} samples, sufficient for collection")
            
            # Stage 2: Block scanning fallback (comprehensive)
            if (len(all_samples) < config.min_samples or 
                (config.fallback_to_block_scanning and len(all_samples) < int(config.sample_limit * config.block_scanning_threshold))):
                
                block_samples, block_metadata = await self._collect_from_block_scanning(config, collection_id)
                metadata["stages"]["block_scanning"] = block_metadata
                
                # Combine samples, avoiding duplicates
                existing_signatures = {s.signature for s in all_samples}
                new_samples = [s for s in block_samples if s.signature not in existing_signatures]
                all_samples.extend(new_samples)
            
            # Limit to sample_limit
            if len(all_samples) > config.sample_limit:
                all_samples = all_samples[:config.sample_limit]
            
            # Update metadata
            metadata["samples_collected"] = len(all_samples)
            metadata["completed_at"] = datetime.utcnow().isoformat()
            metadata["duration_seconds"] = time.time() - start_time
            
            # Verify we have enough samples
            if len(all_samples) < config.min_samples:
                raise Exception(f"Insufficient samples collected: {len(all_samples)} < {config.min_samples}")
            
            return all_samples, metadata
            
        except Exception as e:
            metadata["error"] = str(e)
            metadata["failed_at"] = datetime.utcnow().isoformat()
            raise
    
    async def _collect_from_account_surfaces(
        self,
        config: HybridCollectionConfig,
        collection_id: str
    ) -> Tuple[List[FeeSample], Dict[str, Any]]:
        """Collect from account surfaces using the fixed collector."""
        try:
            # Get account surfaces for the DEX
            account_addresses = self.dex_surfaces.get_addresses_for_dex(config.tx_family, min_confidence=0.5)
            
            if not account_addresses:
                # Fallback to program IDs
                account_addresses = config.program_ids or []
            
            # Create collection config for account surfaces
            surface_config = CollectionConfig(
                chain=config.chain,
                network=config.network,
                tx_family=config.tx_family,
                program_ids=account_addresses,
                time_window_seconds=config.time_window_seconds,
                sample_limit=config.sample_limit,
                min_samples=config.min_samples // 2  # Lower threshold for surface collection
            )
            
            # Collect using fixed collector
            samples, metadata = await self.solana_collector.collect_fee_samples(surface_config, collection_id)
            
            # Update metadata
            metadata.update({
                "method": "account_surfaces",
                "addresses_queried": account_addresses,
                "addresses_count": len(account_addresses),
                "success": len(samples) > 0
            })
            
            return samples, metadata
            
        except Exception as e:
            metadata = {
                "method": "account_surfaces",
                "error": str(e),
                "success": False
            }
            return [], metadata
    
    async def _collect_from_block_scanning(
        self,
        config: HybridCollectionConfig,
        collection_id: str
    ) -> Tuple[List[FeeSample], Dict[str, Any]]:
        """Collect from block scanning."""
        try:
            # Create block scanning config
            scan_config = BlockScanConfig(
                chain=config.chain,
                network=config.network,
                tx_family=config.tx_family,
                target_program_ids=config.program_ids,
                time_window_seconds=config.time_window_seconds,
                sample_limit=config.sample_limit,
                min_samples=config.min_samples,
                max_blocks_to_scan=50,  # Conservative limit
                scan_batch_size=5
            )
            
            # Collect using block scanner
            samples, metadata = await self.block_scanner.collect_fee_samples_block_scanning(scan_config, collection_id)
            
            # Update metadata
            metadata.update({
                "method": "block_scanning",
                "success": len(samples) > 0
            })
            
            return samples, metadata
            
        except Exception as e:
            metadata = {
                "method": "block_scanning",
                "error": str(e),
                "success": False
            }
            return [], metadata
    
    async def verify_signatures_hybrid(
        self,
        signatures: List[str],
        search_transaction_history: bool = True
    ) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
        """Verify signatures using the fixed collector."""
        return await self.solana_collector.verify_signatures(signatures, search_transaction_history)
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get hybrid collection statistics."""
        return {
            "method": "hybrid_collection",
            "strategy": "account_surfaces_first + block_scanning_fallback",
            "supported_dexes": self.dex_surfaces.get_supported_dexes(),
            "account_surfaces_count": len(self.dex_surfaces.get_all_surfaces()),
            "capabilities": {
                "fast_account_surface_queries": True,
                "comprehensive_block_scanning": True,
                "intelligent_fallback": True,
                "duplicate_deduplication": True,
                "sample_limit_enforcement": True
            },
            "components": {
                "solana_collector": "fixed_v1.0",
                "block_scanner": "comprehensive_v1.0",
                "dex_surfaces": "high_signal_v1.0"
            }
        }
    
    async def close(self):
        """Close all services."""
        await self.solana_collector.close()
        await self.block_scanner.close()

# Factory function
def get_hybrid_collector() -> HybridCollector:
    """Get hybrid collector instance."""
    return HybridCollector()
