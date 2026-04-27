"""
Endpointless Provenance API Service

Implements the Gas Memory Post concept:
A content-addressed API response that no longer depends on your server.

The artifact itself becomes the endpoint:
ipfs://Qm... → artifact returns itself
"""
import json
import hashlib
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime

from app.models.gas_memory_post import (
    GasMemoryPost, 
    create_gas_memory_post,
    FeeStatistics,
    VerificationInfo,
    StorageReferences,
    ProvenanceStatus
)
from app.services.hash_delta import CanonicalJSON
from app.services.cross_storage_mirroring import get_cross_storage_mirror, StorageNetwork
from app.utils.config import settings

class EndpointlessProvenanceAPI:
    """
    Service for creating and managing endpointless provenance APIs.
    
    Core concept: The artifact itself becomes the endpoint.
    No server dependency - content-addressed and self-contained.
    """
    
    def __init__(self):
        self.posts: Dict[str, GasMemoryPost] = {}  # Local cache
        self.mirror_service = None
        self._init_mirror_service()
    
    def _init_mirror_service(self):
        """Initialize cross-storage mirroring service."""
        config = {
            'pinata': {
                'api_key': settings.PINATA_API_KEY,
                'secret_key': settings.PINATA_SECRET_KEY,
                'jwt_token': settings.PINATA_JWT
            }
        }
        self.mirror_service = get_cross_storage_mirror(config)
    
    async def create_gas_memory_post(
        self,
        chain: str,
        network: str,
        tx_family: str,
        claim: str,
        fee_samples: List[Dict[str, Any]],
        source: str = "solana_rpc",
        enable_ipfs: bool = True,
        enable_analytics: bool = True
    ) -> GasMemoryPost:
        """
        Create a new Gas Memory Post with content addressing.
        
        Args:
            chain: Blockchain name (e.g., "solana")
            network: Network identifier (e.g., "mainnet-beta")
            tx_family: Transaction family (e.g., "jupiter_swap")
            claim: Human-readable claim about fee behavior
            fee_samples: List of fee sample data
            source: Data source identifier
            enable_ipfs: Whether to store on IPFS
            enable_analytics: Whether to include analytical insights
            
        Returns:
            GasMemoryPost with content addressing
        """
        # Calculate fee statistics
        fee_stats = self._calculate_fee_statistics(fee_samples)
        
        # Create verification info
        verification_info = VerificationInfo(
            source=source,
            methods=["getTransaction", "getSignatureStatuses", "getRecentPrioritizationFees"],
            verification_timestamp=datetime.utcnow(),
            verification_proofs=None,  # Will be populated if verification is done
            provider_stats=None
        )
        
        # Create storage references (initially local)
        storage_refs = StorageReferences(
            ipfs_cid=None,
            arweave_txid=None,
            gateway_urls={},
            pinned=False,
            storage_type="local_dev_store"
        )
        
        # Generate analytical insights if enabled
        analytical_insights = None
        risk_assessment = None
        if enable_analytics:
            analytical_insights = self._generate_analytical_insights(fee_stats)
            risk_assessment = self._assess_risk(fee_stats)
        
        # Create the post
        post = create_gas_memory_post(
            chain=chain,
            network=network,
            tx_family=tx_family,
            claim=claim,
            fee_statistics=fee_stats,
            source=source,
            storage_references=storage_refs,
            verification_info=verification_info,
            analytical_insights=analytical_insights,
            risk_assessment=risk_assessment
        )
        
        # Calculate canonical hash
        canonical_json = CanonicalJSON.serialize(post.model_dump(exclude={'canonical_sha256', 'content_hash'}))
        content_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
        
        # Update post with hash
        post.canonical_sha256 = content_hash
        post.content_hash = content_hash
        
        # Store locally
        self.posts[content_hash] = post
        
        # Mirror to IPFS if enabled
        if enable_ipfs:
            await self._mirror_to_ipfs(post)
        
        return post
    
    async def _mirror_to_ipfs(self, post: GasMemoryPost):
        """Mirror a Gas Memory Post to IPFS."""
        if not self.mirror_service:
            return
        
        try:
            # Serialize post to canonical JSON
            canonical_json = CanonicalJSON.serialize(post.model_dump())
            content_bytes = canonical_json.encode('utf-8')
            
            # Create mirror job
            job = await self.mirror_service.mirror_artifact(
                artifact_hash=post.canonical_sha256,
                content=content_bytes,
                target_networks=[StorageNetwork.IPFS]
            )
            
            # Wait for completion (with timeout)
            max_wait = 30  # seconds
            wait_interval = 1
            waited = 0
            
            while waited < max_wait:
                await asyncio.sleep(wait_interval)
                waited += wait_interval
                
                current_job = self.mirror_service.get_job_status(post.canonical_sha256)
                if current_job and current_job.status in ["completed", "partial", "failed"]:
                    break
            
            # Update post with IPFS information
            final_job = self.mirror_service.get_job_status(post.canonical_sha256)
            if final_job and final_job.results:
                ipfs_result = final_job.results.get("ipfs")
                if ipfs_result and ipfs_result.success:
                    # Update storage references
                    post.storage_references.ipfs_cid = ipfs_result.identifier
                    post.storage_references.gateway_urls["ipfs"] = ipfs_result.gateway_url
                    post.storage_references.pinned = True
                    post.storage_references.storage_type = "ipfs_pinned"
                    
                    # Update provenance status
                    if post.fee_statistics.verified_samples == post.fee_statistics.sample_count:
                        post.provenance_status = ProvenanceStatus.IPFS_PINNED_VERIFIED
                    else:
                        post.provenance_status = ProvenanceStatus.IPFS_PINNED_UNVERIFIED_SAMPLES
                    
                    # Update local cache
                    self.posts[post.canonical_sha256] = post
                    
                    print(f"[Endpointless] Post mirrored to IPFS: {ipfs_result.identifier}")
                else:
                    print(f"[Endpointless] IPFS mirroring failed: {ipfs_result.error}")
            
        except Exception as e:
            print(f"[Endpointless] Error mirroring to IPFS: {str(e)}")
    
    async def resolve_post_by_cid(self, cid: str) -> Optional[GasMemoryPost]:
        """
        Resolve a Gas Memory Post by IPFS CID.
        
        This is the core of the endpointless API:
        ipfs://Qm... → artifact returns itself
        """
        try:
            if not self.mirror_service:
                return None
            
            # Retrieve content from IPFS
            identifier = f"ipfs://{cid}" if not cid.startswith("ipfs://") else cid
            content_bytes = await self.mirror_service._retrieve_from_network(identifier)
            
            if not content_bytes:
                return None
            
            # Parse JSON content
            content_json = content_bytes.decode('utf-8')
            content_data = json.loads(content_json)
            
            # Verify content hash matches
            expected_hash = content_data.get("canonical_sha256")
            if not expected_hash:
                return None
            
            # Recalculate hash to verify integrity
            canonical_json = CanonicalJSON.serialize(content_data)
            actual_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
            
            if actual_hash != expected_hash:
                print(f"[Endpointless] Hash mismatch for CID {cid}: expected {expected_hash}, got {actual_hash}")
                return None
            
            # Create GasMemoryPost object
            post = GasMemoryPost(**content_data)
            
            # Cache locally
            self.posts[actual_hash] = post
            
            return post
            
        except Exception as e:
            print(f"[Endpointless] Error resolving CID {cid}: {str(e)}")
            return None
    
    async def verify_post_integrity(self, post: GasMemoryPost) -> Dict[str, Any]:
        """
        Verify the integrity of a Gas Memory Post.
        
        Checks:
        - Content hash matches
        - Storage references are valid
        - IPFS content is retrievable and matches
        """
        verification_result = {
            "content_hash_valid": False,
            "storage_references_valid": False,
            "ipfs_retrievable": False,
            "ipfs_content_matches": False,
            "overall_valid": False,
            "details": {}
        }
        
        try:
            # Verify content hash
            canonical_json = CanonicalJSON.serialize(post.model_dump(exclude={'canonical_sha256', 'content_hash'}))
            calculated_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
            
            hash_valid = calculated_hash == post.canonical_sha256
            verification_result["content_hash_valid"] = hash_valid
            verification_result["details"]["hash_check"] = {
                "expected": post.canonical_sha256,
                "calculated": calculated_hash,
                "valid": hash_valid
            }
            
            # Verify storage references
            storage_valid = True
            storage_details = {}
            
            if post.storage_references.ipfs_cid:
                # Try to retrieve from IPFS
                try:
                    retrieved_post = await self.resolve_post_by_cid(post.storage_references.ipfs_cid.replace("ipfs://", ""))
                    storage_valid = retrieved_post is not None
                    verification_result["ipfs_retrievable"] = storage_valid
                    
                    if retrieved_post:
                        content_matches = retrieved_post.canonical_sha256 == post.canonical_sha256
                        verification_result["ipfs_content_matches"] = content_matches
                        storage_details["ipfs"] = {
                            "cid": post.storage_references.ipfs_cid,
                            "gateway_url": post.get_ipfs_url(),
                            "retrievable": storage_valid,
                            "content_matches": content_matches
                        }
                except Exception as e:
                    storage_details["ipfs"] = {"error": str(e)}
                    storage_valid = False
            
            verification_result["storage_references_valid"] = storage_valid
            verification_result["details"]["storage_check"] = storage_details
            
            # Overall validity
            verification_result["overall_valid"] = (
                verification_result["content_hash_valid"] and
                verification_result["storage_references_valid"]
            )
            
        except Exception as e:
            verification_result["error"] = str(e)
        
        return verification_result
    
    def get_post_by_hash(self, content_hash: str) -> Optional[GasMemoryPost]:
        """Get a cached post by content hash."""
        return self.posts.get(content_hash)
    
    def list_posts(self, limit: int = 100) -> List[GasMemoryPost]:
        """List cached posts."""
        return list(self.posts.values())[:limit]
    
    def search_posts(
        self,
        chain: Optional[str] = None,
        tx_family: Optional[str] = None,
        provenance_status: Optional[ProvenanceStatus] = None,
        min_success_rate: Optional[float] = None
    ) -> List[GasMemoryPost]:
        """Search cached posts with filters."""
        filtered_posts = []
        
        for post in self.posts.values():
            # Chain filter
            if chain and post.chain.name != chain:
                continue
            
            # Transaction family filter
            if tx_family and post.tx_family != tx_family:
                continue
            
            # Provenance status filter
            if provenance_status and post.provenance_status != provenance_status:
                continue
            
            # Success rate filter
            if min_success_rate is not None and post.fee_statistics.success_rate < min_success_rate:
                continue
            
            filtered_posts.append(post)
        
        return filtered_posts
    
    def _calculate_fee_statistics(self, fee_samples: List[Dict[str, Any]]) -> FeeStatistics:
        """Calculate fee statistics from sample data."""
        if not fee_samples:
            return FeeStatistics(
                sample_count=0,
                verified_samples=0,
                success_rate=0.0,
                median_latency_slots=0.0,
                p50_micro_lamports_per_cu=0,
                p75_micro_lamports_per_cu=0,
                p90_micro_lamports_per_cu=0
            )
        
        # Extract metrics
        success_count = sum(1 for s in fee_samples if s.get("success", False))
        total_count = len(fee_samples)
        success_rate = success_count / total_count if total_count > 0 else 0.0
        
        # Latency statistics
        latencies = [s.get("confirmation_latency_slots", 0) for s in fee_samples if s.get("success", False)]
        latencies.sort()
        median_latency = latencies[len(latencies) // 2] if latencies else 0.0
        
        # Fee per compute unit statistics
        fees_per_cu = []
        for s in fee_samples:
            if s.get("success", False) and s.get("compute_units_consumed", 0) > 0:
                fee_per_cu = s.get("total_fee_lamports", 0) / s.get("compute_units_consumed", 1)
                fees_per_cu.append(fee_per_cu)
        
        if fees_per_cu:
            fees_per_cu.sort()
            n = len(fees_per_cu)
            p50 = int(fees_per_cu[int(n * 0.5)])
            p75 = int(fees_per_cu[int(n * 0.75)])
            p90 = int(fees_per_cu[int(n * 0.9)])
        else:
            p50 = p75 = p90 = 0
        
        return FeeStatistics(
            sample_count=total_count,
            verified_samples=success_count,  # Assuming all successful samples are verified
            success_rate=success_rate,
            median_latency_slots=median_latency,
            p50_micro_lamports_per_cu=p50,
            p75_micro_lamports_per_cu=p75,
            p90_micro_lamports_per_cu=p90
        )
    
    def _generate_analytical_insights(self, fee_stats: FeeStatistics) -> Dict[str, Any]:
        """Generate analytical insights from fee statistics."""
        # Simple scoring based on success rate and latency
        execution_score = (
            fee_stats.success_rate * 0.6 +
            (1.0 / (1.0 + fee_stats.median_latency_slots)) * 0.4
        )
        
        # Risk assessment
        if fee_stats.success_rate >= 0.95:
            risk_level = "low"
        elif fee_stats.success_rate >= 0.85:
            risk_level = "medium"
        else:
            risk_level = "high"
        
        # Recommended policy
        if risk_level == "low":
            recommended_policy = {"cheap": "p50", "balanced": "p75", "urgent": "p90"}
        elif risk_level == "medium":
            recommended_policy = {"cheap": "p75", "balanced": "p90", "urgent": "p95"}
        else:
            recommended_policy = {"cheap": "p90", "balanced": "p95", "urgent": "p99"}
        
        return {
            "execution_score": round(execution_score, 3),
            "risk_level": risk_level,
            "recommended_policy": recommended_policy,
            "confidence": min(fee_stats.sample_count / 1000.0, 1.0),
            "sample_adequacy": "adequate" if fee_stats.sample_count >= 100 else "limited"
        }
    
    def _assess_risk(self, fee_stats: FeeStatistics) -> str:
        """Generate risk assessment string."""
        if fee_stats.success_rate >= 0.95:
            return "Low risk - high success rate with predictable fee patterns"
        elif fee_stats.success_rate >= 0.85:
            return "Medium risk - moderate success rate, consider higher fees for reliability"
        elif fee_stats.success_rate >= 0.70:
            return "High risk - low success rate, use urgent fee strategy"
        else:
            return "Very high risk - unpredictable execution, consider different transaction family"
    
    async def close(self):
        """Close services."""
        if self.mirror_service:
            await self.mirror_service.close()

# Factory function
def get_endpointless_provenance_api() -> EndpointlessProvenanceAPI:
    """Get endpointless provenance API service instance."""
    return EndpointlessProvenanceAPI()
