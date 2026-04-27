"""
Trustless Third-Party Verification System

Enables independent verification of Gas Memory artifacts without trusting the original server.
Anyone can verify:
1. Content integrity via SHA-256
2. Signature verification via public RPC
3. Provider disagreement detection
4. Mathematical consistency of fee statistics
"""
import hashlib
import json
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import httpx

from app.models.gas_memory_post import GasMemoryPost
from app.services.canonical_artifact import CanonicalJSONSerializer

class TrustlessVerifier:
    """
    Trustless verification system for Gas Memory artifacts.
    
    Verification layers:
    1. Content integrity - SHA-256 hash verification
    2. Signature verification - Independent RPC verification
    3. Statistical consistency - Mathematical verification of fee curves
    4. Provider disagreement - Cross-provider verification detection
    """
    
    def __init__(self):
        self.serializer = CanonicalJSONSerializer()
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Public RPC endpoints for independent verification
        self.public_rpcs = [
            "https://api.mainnet-beta.solana.com",
            "https://solana-api.projectserum.com",
            "https://rpc.ankr.com/solana",
            "https://solana-mainnet.rpc.explorer.network"
        ]
    
    async def verify_artifact_completely(
        self,
        artifact_content: Dict[str, Any],
        content_hash: str
    ) -> Dict[str, Any]:
        """
        Complete trustless verification of artifact.
        
        Returns comprehensive verification report.
        """
        verification_report = {
            "artifact_hash": content_hash,
            "verification_timestamp": datetime.utcnow().isoformat(),
            "verifications": {},
            "overall_verified": False,
            "trust_score": 0.0,
            "issues": [],
            "warnings": []
        }
        
        try:
            # 1. Content integrity verification
            content_verification = await self._verify_content_integrity(
                artifact_content, content_hash
            )
            verification_report["verifications"]["content_integrity"] = content_verification
            
            # 2. Signature verification
            signature_verification = await self._verify_signatures_independently(
                artifact_content
            )
            verification_report["verifications"]["signature_verification"] = signature_verification
            
            # 3. Statistical consistency
            stats_verification = await self._verify_statistical_consistency(
                artifact_content
            )
            verification_report["verifications"]["statistical_consistency"] = stats_verification
            
            # 4. Provider disagreement detection
            provider_verification = await self._detect_provider_disagreement(
                artifact_content
            )
            verification_report["verifications"]["provider_analysis"] = provider_verification
            
            # Calculate overall trust score
            trust_score = self._calculate_trust_score(verification_report["verifications"])
            verification_report["trust_score"] = trust_score
            
            # Determine overall verification
            verification_report["overall_verified"] = trust_score >= 0.8
            
            # Collect issues and warnings
            for verification in verification_report["verifications"].values():
                if verification.get("issues"):
                    verification_report["issues"].extend(verification["issues"])
                if verification.get("warnings"):
                    verification_report["warnings"].extend(verification["warnings"])
            
            return verification_report
            
        except Exception as e:
            verification_report["error"] = str(e)
            verification_report["overall_verified"] = False
            return verification_report
    
    async def _verify_content_integrity(
        self,
        artifact_content: Dict[str, Any],
        expected_hash: str
    ) -> Dict[str, Any]:
        """Verify content integrity via canonical SHA-256."""
        verification = {
            "method": "content_integrity",
            "verified": False,
            "expected_hash": expected_hash,
            "computed_hash": None,
            "matches": False,
            "issues": [],
            "warnings": []
        }
        
        try:
            # Compute canonical hash
            computed_hash = self.serializer.hash_content(artifact_content)
            verification["computed_hash"] = computed_hash
            
            # Compare hashes
            verification["matches"] = computed_hash == expected_hash
            verification["verified"] = verification["matches"]
            
            if not verification["matches"]:
                verification["issues"].append(
                    f"Hash mismatch: expected {expected_hash}, got {computed_hash}"
                )
            
        except Exception as e:
            verification["issues"].append(f"Content integrity verification failed: {str(e)}")
        
        return verification
    
    async def _verify_signatures_independently(
        self,
        artifact_content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify signatures independently via public RPC endpoints."""
        verification = {
            "method": "signature_verification",
            "verified": False,
            "total_signatures": 0,
            "verified_signatures": 0,
            "verification_rate": 0.0,
            "provider_results": {},
            "disagreements": [],
            "issues": [],
            "warnings": []
        }
        
        try:
            # Extract verification proofs from artifact
            verification_proofs = artifact_content.get("verification", {}).get("verification_proofs", [])
            
            if not verification_proofs:
                verification["warnings"].append("No verification proofs found in artifact")
                return verification
            
            verification["total_signatures"] = len(verification_proofs)
            
            # Sample signatures for verification (verify max 50 for performance)
            sample_proofs = verification_proofs[:50]
            
            # Verify each signature across multiple providers
            for proof in sample_proofs:
                signature = proof.get("signature")
                if not signature:
                    continue
                
                signature_results = await self._verify_signature_across_providers(signature)
                verification["provider_results"][signature] = signature_results
                
                # Count successful verifications
                if any(result.get("verified", False) for result in signature_results.values()):
                    verification["verified_signatures"] += 1
                
                # Check for provider disagreements
                verified_providers = [
                    provider for provider, result in signature_results.items()
                    if result.get("verified", False)
                ]
                
                if len(verified_providers) > 0 and len(verified_providers) < len(signature_results):
                    verification["disagreements"].append({
                        "signature": signature,
                        "verified_providers": verified_providers,
                        "failed_providers": [
                            provider for provider, result in signature_results.items()
                            if not result.get("verified", False)
                        ]
                    })
            
            # Calculate verification rate
            if verification["total_signatures"] > 0:
                verification["verification_rate"] = verification["verified_signatures"] / verification["total_signatures"]
            
            # Determine verification status
            verification["verified"] = verification["verification_rate"] >= 0.8
            
            if verification["verification_rate"] < 0.5:
                verification["issues"].append(
                    f"Very low verification rate: {verification['verification_rate']:.1%}"
                )
            elif verification["verification_rate"] < 0.8:
                verification["warnings"].append(
                    f"Low verification rate: {verification['verification_rate']:.1%}"
                )
            
        except Exception as e:
            verification["issues"].append(f"Signature verification failed: {str(e)}")
        
        return verification
    
    async def _verify_signature_across_providers(
        self,
        signature: str
    ) -> Dict[str, Dict[str, Any]]:
        """Verify single signature across multiple public RPC providers."""
        results = {}
        
        for rpc_url in self.public_rpcs:
            try:
                result = await self._verify_signature_with_provider(signature, rpc_url)
                results[rpc_url] = result
            except Exception as e:
                results[rpc_url] = {
                    "verified": False,
                    "error": str(e),
                    "provider": rpc_url
                }
        
        return results
    
    async def _verify_signature_with_provider(
        self,
        signature: str,
        rpc_url: str
    ) -> Dict[str, Any]:
        """Verify signature with specific RPC provider."""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
            }
            
            response = await self.client.post(
                rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10.0
            )
            
            data = response.json()
            
            if "error" not in data and data.get("result"):
                result = data["result"]
                meta = result.get("meta", {})
                
                return {
                    "verified": True,
                    "provider": rpc_url,
                    "slot": result.get("slot"),
                    "blockTime": result.get("blockTime"),
                    "fee": meta.get("fee", 0),
                    "err": meta.get("err"),
                    "success": meta.get("err") is None
                }
            else:
                return {
                    "verified": False,
                    "provider": rpc_url,
                    "error": data.get("error", "Not found")
                }
                
        except Exception as e:
            return {
                "verified": False,
                "provider": rpc_url,
                "error": str(e)
            }
    
    async def _verify_statistical_consistency(
        self,
        artifact_content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify statistical consistency of fee data."""
        verification = {
            "method": "statistical_consistency",
            "verified": False,
            "checks": {},
            "issues": [],
            "warnings": []
        }
        
        try:
            fee_stats = artifact_content.get("fees_statistics", {})
            
            if not fee_stats:
                verification["warnings"].append("No fee statistics found in artifact")
                return verification
            
            # Check 1: p50 <= p75 <= p90
            p50 = fee_stats.get("p50_micro_lamports_per_cu", 0)
            p75 = fee_stats.get("p75_micro_lamports_per_cu", 0)
            p90 = fee_stats.get("p90_micro_lamports_per_cu", 0)
            
            percentile_check = {
                "name": "percentile_ordering",
                "verified": p50 <= p75 <= p90,
                "p50": p50,
                "p75": p75,
                "p90": p90
            }
            
            if not percentile_check["verified"]:
                verification["issues"].append(
                    f"Percentile ordering violated: p50({p50}) <= p75({p75}) <= p90({p90})"
                )
            
            verification["checks"]["percentile_ordering"] = percentile_check
            
            # Check 2: Success rate between 0 and 1
            success_rate = fee_stats.get("success_rate", 0)
            success_rate_check = {
                "name": "success_rate_bounds",
                "verified": 0 <= success_rate <= 1,
                "success_rate": success_rate
            }
            
            if not success_rate_check["verified"]:
                verification["issues"].append(
                    f"Success rate out of bounds: {success_rate}"
                )
            
            verification["checks"]["success_rate_bounds"] = success_rate_check
            
            # Check 3: Sample count consistency
            sample_count = fee_stats.get("sample_count", 0)
            verified_samples = fee_stats.get("verified_samples", 0)
            
            sample_check = {
                "name": "sample_consistency",
                "verified": verified_samples <= sample_count,
                "sample_count": sample_count,
                "verified_samples": verified_samples
            }
            
            if not sample_check["verified"]:
                verification["issues"].append(
                    f"Verified samples ({verified_samples}) > total samples ({sample_count})"
                )
            
            verification["checks"]["sample_consistency"] = sample_check
            
            # Check 4: Reasonable fee ranges (Solana typical fees)
            if p50 > 0:
                fee_range_check = {
                    "name": "fee_range_reasonableness",
                    "verified": 1000 <= p50 <= 1000000,  # 1 to 1000 lamports per CU
                    "p50_micro_lamports": p50
                }
                
                if not fee_range_check["verified"]:
                    verification["warnings"].append(
                        f"Unusual fee range: p50 = {p50} µL/CU"
                    )
                
                verification["checks"]["fee_range_reasonableness"] = fee_range_check
            
            # Overall verification
            all_checks_passed = all(check.get("verified", False) for check in verification["checks"].values())
            verification["verified"] = all_checks_passed and len(verification["issues"]) == 0
            
        except Exception as e:
            verification["issues"].append(f"Statistical consistency check failed: {str(e)}")
        
        return verification
    
    async def _detect_provider_disagreement(
        self,
        artifact_content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Detect provider disagreement in verification data."""
        verification = {
            "method": "provider_disagreement",
            "verified": True,  # Default to true unless major issues
            "provider_stats": {},
            "disagreements": [],
            "issues": [],
            "warnings": []
        }
        
        try:
            # Extract provider stats from verification info
            provider_stats = artifact_content.get("verification", {}).get("provider_stats", {})
            
            if not provider_stats:
                verification["warnings"].append("No provider stats found in artifact")
                return verification
            
            # Analyze each provider's performance
            for provider, stats in provider_stats.items():
                count = stats.get("count", 0)
                verified = stats.get("verified", 0)
                success_rate = verified / count if count > 0 else 0
                
                verification["provider_stats"][provider] = {
                    "total_requests": count,
                    "verified_requests": verified,
                    "success_rate": success_rate
                }
                
                # Flag providers with very low success rates
                if success_rate < 0.5 and count > 10:
                    verification["warnings"].append(
                        f"Provider {provider} has low success rate: {success_rate:.1%}"
                    )
            
            # Check for significant provider performance differences
            success_rates = [
                stats["success_rate"] for stats in verification["provider_stats"].values()
            ]
            
            if len(success_rates) > 1:
                max_rate = max(success_rates)
                min_rate = min(success_rates)
                
                if max_rate - min_rate > 0.3:  # 30% difference
                    verification["disagreements"].append({
                        "type": "performance_disparity",
                        "max_success_rate": max_rate,
                        "min_success_rate": min_rate,
                        "difference": max_rate - min_rate
                    })
                    
                    verification["warnings"].append(
                        f"Significant provider performance disparity: {min_rate:.1%} to {max_rate:.1%}"
                    )
            
        except Exception as e:
            verification["issues"].append(f"Provider disagreement detection failed: {str(e)}")
        
        return verification
    
    def _calculate_trust_score(self, verifications: Dict[str, Any]) -> float:
        """Calculate overall trust score from all verifications."""
        weights = {
            "content_integrity": 0.3,      # Most important
            "signature_verification": 0.3, # Critical for ground truth
            "statistical_consistency": 0.2, # Data quality
            "provider_analysis": 0.2      # Reliability
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for verification_name, weight in weights.items():
            verification = verifications.get(verification_name, {})
            
            if verification.get("verified", False):
                # Base score for passing verification
                score = 1.0
                
                # Penalize for warnings
                warnings = verification.get("warnings", [])
                if warnings:
                    score -= len(warnings) * 0.1
                
                # Heavily penalize for issues
                issues = verification.get("issues", [])
                if issues:
                    score -= len(issues) * 0.3
                
                score = max(0.0, score)  # Don't go below 0
            else:
                # Failed verification gets 0 score
                score = 0.0
            
            total_score += score * weight
            total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    async def verify_cid_standalone(self, cid: str) -> Dict[str, Any]:
        """
        Verify a CID completely standalone.
        
        This is the ultimate test: can anyone verify the artifact
        without trusting the original server?
        """
        verification = {
            "cid": cid,
            "standalone_verification": True,
            "timestamp": datetime.utcnow().isoformat(),
            "steps": {},
            "success": False,
            "trust_score": 0.0
        }
        
        try:
            # Step 1: Retrieve artifact from IPFS
            from app.services.persistent_storage import get_persistent_storage_manager
            storage = get_persistent_storage_manager()
            
            artifact_content = await storage.retrieve_artifact(cid)
            if not artifact_content:
                verification["error"] = "Could not retrieve artifact from IPFS"
                return verification
            
            # Step 2: Parse artifact content
            if isinstance(artifact_content, bytes):
                artifact_data = json.loads(artifact_content.decode('utf-8'))
            else:
                artifact_data = artifact_content
            
            verification["steps"]["retrieval"] = {"success": True, "source": "ipfs"}
            
            # Step 3: Extract content hash
            content_hash = artifact_data.get("canonical_sha256")
            if not content_hash:
                verification["error"] = "No content hash found in artifact"
                return verification
            
            verification["steps"]["hash_extraction"] = {"success": True, "hash": content_hash}
            
            # Step 4: Complete verification
            complete_verification = await self.verify_artifact_completely(
                artifact_data, content_hash
            )
            
            verification["steps"]["complete_verification"] = complete_verification
            verification["success"] = complete_verification["overall_verified"]
            verification["trust_score"] = complete_verification["trust_score"]
            
            return verification
            
        except Exception as e:
            verification["error"] = str(e)
            verification["success"] = False
            return verification
        finally:
            await storage.close()
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

# Factory function
def get_trustless_verifier() -> TrustlessVerifier:
    """Get trustless verifier instance."""
    return TrustlessVerifier()
