"""
Enhanced Solana Fee Collector for Gas Memory System

Collects live Solana transaction fee data with rotating provider pool,
rate limiting, and comprehensive verification support.
"""
import asyncio
import time
import json
import hashlib
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import httpx

from app.services.rpc_provider_pool import get_provider_pool
from app.models.provenance import FeeSample, CollectionStatus
from app.utils.config import settings

@dataclass
class CollectionConfig:
    """Configuration for fee collection."""
    chain: str = "solana"
    network: str = "mainnet-beta"
    tx_family: str = "jupiter_swap"
    program_ids: List[str] = None
    time_window_seconds: int = 3600  # 1 hour
    sample_limit: int = 10000
    min_samples: int = 100
    
    def __post_init__(self):
        if self.program_ids is None:
            # Default Jupiter program IDs
            self.program_ids = [
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaxV",  # Jupiter Program
                "JUP4Fb2cqiRUcaTHrfPC8x2rtjso5zAz2cTqfBxgsQ",   # Jupiter Router
            ]

class SolanaFeeCollectorV2:
    """
    Enhanced Solana fee collector with provider pool and verification support.
    
    Features:
    - Rotating RPC provider pool with health tracking
    - Rate limiting and exponential backoff
    - Comprehensive signature verification
    - Canonical data serialization
    - Idempotent collection with deduplication
    """
    
    def __init__(self):
        self.provider_pool = get_provider_pool()
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Jupiter-specific configuration
        self.jupiter_config = CollectionConfig()
        
        # Cache for recent collections to avoid duplicates
        self.recent_signatures: Dict[str, float] = {}
        self.cache_cleanup_interval = 300  # 5 minutes
        self.last_cache_cleanup = time.time()
    
    async def collect_fee_samples(
        self,
        config: CollectionConfig,
        collection_id: str
    ) -> Tuple[List[FeeSample], Dict[str, Any]]:
        """
        Collect fee samples for specified transaction family.
        
        Args:
            config: Collection configuration
            collection_id: Unique identifier for this collection
            
        Returns:
            Tuple of (samples, metadata)
        """
        start_time = time.time()
        metadata = {
            "collection_id": collection_id,
            "config": config.__dict__,
            "started_at": datetime.utcnow().isoformat(),
            "providers_used": [],
            "signatures_found": 0,
            "samples_collected": 0,
            "verification_status": "pending"
        }
        
        try:
            # Get signatures for the transaction family
            signatures = await self._get_signatures_for_family(config, metadata)
            metadata["signatures_found"] = len(signatures)
            
            if not signatures:
                return [], metadata
            
            # Collect transaction details
            samples = await self._collect_transaction_details(signatures, config, metadata)
            metadata["samples_collected"] = len(samples)
            
            # Update metadata
            metadata["completed_at"] = datetime.utcnow().isoformat()
            metadata["duration_seconds"] = time.time() - start_time
            
            return samples, metadata
            
        except Exception as e:
            metadata["error"] = str(e)
            metadata["failed_at"] = datetime.utcnow().isoformat()
            raise
    
    async def _get_signatures_for_family(
        self,
        config: CollectionConfig,
        metadata: Dict[str, Any]
    ) -> List[str]:
        """Get transaction signatures for the specified transaction family."""
        signatures = []
        
        # Calculate time window
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(seconds=config.time_window_seconds)
        
        # For Jupiter swaps, we can use getSignaturesForAddress
        # This is more efficient than searching all transactions
        for program_id in config.program_ids:
            try:
                program_signatures = await self._get_signatures_for_address(
                    program_id,
                    start_time,
                    end_time,
                    config.sample_limit // len(config.program_ids),
                    metadata
                )
                signatures.extend(program_signatures)
                
                # Update metadata
                metadata["providers_used"].append({
                    "program_id": program_id,
                    "signatures_found": len(program_signatures)
                })
                
            except Exception as e:
                print(f"[Collector] Error getting signatures for {program_id}: {str(e)}")
                metadata["providers_used"].append({
                    "program_id": program_id,
                    "error": str(e)
                })
        
        # Remove duplicates and limit
        unique_signatures = list(set(signatures))
        return unique_signatures[:config.sample_limit]
    
    async def _get_signatures_for_address(
        self,
        address: str,
        start_time: datetime,
        end_time: datetime,
        limit: int,
        metadata: Dict[str, Any]
    ) -> List[str]:
        """Get signatures for a specific address within time window."""
        signatures = []
        
        # Convert to Unix timestamps
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())
        
        # Use provider pool with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Get healthy provider
                provider = self.provider_pool.get_healthy_provider()
                if not provider:
                    raise Exception("No healthy providers available")
                
                # Build request
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [
                        address,
                        {
                            "limit": limit,
                            "before": end_timestamp,
                            "until": start_timestamp
                        }
                    ]
                }
                
                # Make request
                start_request = time.time()
                response = await self.client.post(
                    provider.url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response_time_ms = (time.time() - start_request) * 1000
                
                # Record provider usage
                self.provider_pool.record_request(provider.name, response_time_ms, success=True)
                
                # Parse response
                data = response.json()
                if "error" in data:
                    raise Exception(f"RPC error: {data['error']}")
                
                result = data.get("result", [])
                signatures = [item["signature"] for item in result if "signature" in item]
                
                # Update metadata
                metadata["providers_used"][-1]["provider"] = provider.name
                metadata["providers_used"][-1]["response_time_ms"] = response_time_ms
                
                break
                
            except Exception as e:
                if attempt < max_retries - 1:
                    # Record failure and try next provider
                    if 'provider' in locals():
                        self.provider_pool.record_request(provider.name, 0, success=False)
                    
                    # Exponential backoff
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                else:
                    raise Exception(f"Failed after {max_retries} attempts: {str(e)}")
        
        return signatures
    
    async def _collect_transaction_details(
        self,
        signatures: List[str],
        config: CollectionConfig,
        metadata: Dict[str, Any]
    ) -> List[FeeSample]:
        """Collect detailed transaction information for signatures."""
        samples = []
        
        # Process in batches to avoid rate limiting
        batch_size = 25
        for i in range(0, len(signatures), batch_size):
            batch = signatures[i:i + batch_size]
            
            try:
                batch_samples = await self._process_signature_batch(batch, config)
                samples.extend(batch_samples)
                
                # Rate limiting delay
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"[Collector] Error processing batch {i//batch_size}: {str(e)}")
                continue
        
        # Deduplicate samples
        unique_samples = {}
        for sample in samples:
            if sample.signature not in unique_samples:
                unique_samples[sample.signature] = sample
        
        return list(unique_samples.values())
    
    async def _process_signature_batch(
        self,
        signatures: List[str],
        config: CollectionConfig
    ) -> List[FeeSample]:
        """Process a batch of signatures to extract fee data."""
        samples = []
        
        # Use provider pool for batch processing
        provider = self.provider_pool.get_healthy_provider()
        if not provider:
            raise Exception("No healthy providers available")
        
        # Build batch request for getTransaction
        batch_payload = []
        for i, signature in enumerate(signatures):
            batch_payload.append({
                "jsonrpc": "2.0",
                "id": i + 1,
                "method": "getTransaction",
                "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
            })
        
        try:
            # Make batch request
            start_request = time.time()
            response = await self.client.post(
                provider.url,
                json=batch_payload,
                headers={"Content-Type": "application/json"}
            )
            response_time_ms = (time.time() - start_request) * 1000
            
            # Record provider usage
            self.provider_pool.record_request(provider.name, response_time_ms, success=True)
            
            # Parse batch response
            if response.headers.get("content-type", "").startswith("application/json"):
                # Single JSON response (some providers don't support batch)
                data = response.json()
                if isinstance(data, list):
                    responses = data
                else:
                    # Single response, process first signature only
                    responses = [data]
            else:
                # Line-separated JSON responses
                responses = []
                for line in response.text.splitlines():
                    if line.strip():
                        responses.append(json.loads(line))
            
            # Process each response
            for i, resp in enumerate(responses):
                if i < len(signatures):
                    signature = signatures[i]
                    sample = self._parse_transaction_response(signature, resp, provider.name, config)
                    if sample:
                        samples.append(sample)
            
        except Exception as e:
            # Record failure
            self.provider_pool.record_request(provider.name, 0, success=False)
            raise
        
        return samples
    
    def _parse_transaction_response(
        self,
        signature: str,
        response: Dict[str, Any],
        provider_name: str,
        config: CollectionConfig
    ) -> Optional[FeeSample]:
        """Parse transaction response to extract fee data."""
        try:
            # Check for errors
            if "error" in response:
                return None
            
            result = response.get("result")
            if not result:
                return None
            
            # Extract transaction data
            meta = result.get("meta", {})
            transaction = result.get("transaction", {})
            message = transaction.get("message", {})
            
            # Calculate confirmation latency
            slot = result.get("slot", 0)
            current_slot = self._get_current_slot_estimate()
            latency_slots = max(0, current_slot - slot)
            
            # Extract fee information
            fee_info = meta.get("fee", 0)
            pre_balances = meta.get("preBalances", [])
            post_balances = meta.get("postBalances", [])
            
            # Calculate priority fee (difference between actual fee and base fee)
            base_fee = meta.get("fee", 0)  # Simplified - actual calculation more complex
            priority_fee = max(0, fee_info - base_fee)
            
            # Extract compute unit information
            compute_units_consumed = meta.get("computeUnitsConsumed", 0)
            compute_unit_limit = message.get("instructions", [{}])[-1].get("computeUnitLimit", 0)
            
            # Calculate fee per compute unit
            if compute_units_consumed > 0:
                fee_per_cu = (fee_info * 1_000_000) // compute_units_consumed  # Convert to micro-lamports
            else:
                fee_per_cu = 0
            
            # Determine success
            err = meta.get("err")
            success = err is None
            
            # Extract program IDs
            program_ids = []
            instructions = message.get("instructions", [])
            for instruction in instructions:
                if "programIdIndex" in instruction:
                    program_id_index = instruction["programIdIndex"]
                    if program_id_index < len(message.get("accountKeys", [])):
                        program_id = message["accountKeys"][program_id_index]
                        program_ids.append(program_id)
            
            # Filter by target program IDs
            if config.program_ids:
                if not any(pid in program_ids for pid in config.program_ids):
                    return None
            
            # Create sample
            sample = FeeSample(
                signature=signature,
                slot=slot,
                block_time=datetime.fromtimestamp(result.get("blockTime", time.time())),
                compute_units_consumed=compute_units_consumed,
                compute_unit_limit=compute_unit_limit,
                compute_unit_price_micro_lamports=fee_per_cu,
                priority_fee_lamports=priority_fee,
                base_fee_lamports=base_fee,
                total_fee_lamports=fee_info,
                confirmation_latency_slots=latency_slots,
                success=success,
                program_ids=program_ids,
                transaction_type=config.tx_family,
                verified=False,  # Will be set during verification
                source_provider=provider_name,
                collected_at=datetime.utcnow()
            )
            
            return sample
            
        except Exception as e:
            print(f"[Collector] Error parsing transaction {signature}: {str(e)}")
            return None
    
    def _get_current_slot_estimate(self) -> int:
        """Get estimated current slot (simplified)."""
        # This should use getSlot() from RPC, but for now use a rough estimate
        # Solana produces ~400ms per slot
        slots_per_second = 2.5
        current_time = time.time()
        # Approximate genesis time (can be made more accurate)
        genesis_time = 1598550000  # August 2020
        estimated_slot = int((current_time - genesis_time) * slots_per_second)
        return estimated_slot
    
    async def verify_signatures(
        self,
        signatures: List[str],
        search_transaction_history: bool = True
    ) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
        """
        Verify signatures using getSignatureStatuses with fallback.
        
        Returns:
            Tuple of (verification_results, provider_name, metadata)
        """
        metadata = {
            "method": "getSignatureStatuses",
            "signatures_total": len(signatures),
            "search_transaction_history": search_transaction_history,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            # Use provider pool for verification
            provider = self.provider_pool.get_healthy_provider()
            if not provider:
                raise Exception("No healthy providers available")
            
            # Build request
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignatureStatuses",
                "params": [
                    signatures,
                    {"searchTransactionHistory": search_transaction_history}
                ]
            }
            
            # Make request
            start_request = time.time()
            response = await self.client.post(
                provider.url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response_time_ms = (time.time() - start_request) * 1000
            
            # Record provider usage
            self.provider_pool.record_request(provider.name, response_time_ms, success=True)
            
            # Parse response
            data = response.json()
            if "error" in data:
                raise Exception(f"RPC error: {data['error']}")
            
            results = data.get("result", [])
            
            # Build verification results
            verification_results = []
            for i, status in enumerate(results):
                if i < len(signatures):
                    verification_results.append({
                        "signature": signatures[i],
                        "status": status,
                        "verified": status is not None,
                        "provider": provider.name
                    })
            
            metadata.update({
                "provider": provider.name,
                "response_time_ms": response_time_ms,
                "verified_count": sum(1 for r in verification_results if r["verified"])
            })
            
            return verification_results, provider.name, metadata
            
        except Exception as e:
            metadata["error"] = str(e)
            if 'provider' in locals():
                self.provider_pool.record_request(provider.name, 0, success=False)
            raise
    
    async def get_transaction_with_fallback(self, signature: str) -> Tuple[Optional[Dict[str, Any]], str, Dict[str, Any]]:
        """
        Get transaction with fallback across providers.
        
        Returns:
            Tuple of (transaction_data, provider_name, metadata)
        """
        metadata = {
            "method": "getTransaction",
            "signature": signature,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Try multiple providers
        for attempt in range(3):
            try:
                provider = self.provider_pool.get_healthy_provider()
                if not provider:
                    break
                
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                }
                
                start_request = time.time()
                response = await self.client.post(
                    provider.url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                response_time_ms = (time.time() - start_request) * 1000
                
                # Record provider usage
                self.provider_pool.record_request(provider.name, response_time_ms, success=True)
                
                data = response.json()
                if "error" not in data and data.get("result"):
                    metadata.update({
                        "provider": provider.name,
                        "response_time_ms": response_time_ms,
                        "found": True
                    })
                    return data, provider.name, metadata
                else:
                    metadata.update({
                        "provider": provider.name,
                        "error": data.get("error"),
                        "found": False
                    })
                
            except Exception as e:
                metadata["error"] = str(e)
                if 'provider' in locals():
                    self.provider_pool.record_request(provider.name, 0, success=False)
                continue
        
        return None, "failed", metadata
    
    def get_provider_stats(self) -> Dict[str, Any]:
        """Get provider pool statistics."""
        return self.provider_pool.get_provider_stats()
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

# Factory function
def get_solana_collector_v2() -> SolanaFeeCollectorV2:
    """Get enhanced Solana fee collector instance."""
    return SolanaFeeCollectorV2()
