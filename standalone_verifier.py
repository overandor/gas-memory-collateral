"""
Standalone CID Verifier

The ultimate test: Delete your server. Can I still:
- fetch the artifact from IPFS
- verify signatures independently  
- recompute SHA-256
- confirm provider disagreement
- trust the data without you?

This is what makes Gas Memory truly endpointless.
"""
import json
import hashlib
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio

class StandaloneVerifier:
    """
    Standalone verifier that works completely independently.
    
    No original server required. No trust in the creator.
    Just math, public RPCs, and IPFS.
    """
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Public IPFS gateways (no trust required)
        self.ipfs_gateways = [
            "https://gateway.pinata.cloud/ipfs/",
            "https://ipfs.io/ipfs/",
            "https://cloudflare-ipfs.com/ipfs/",
            "https://infura-ipfs.io/ipfs/"
        ]
        
        # Public Solana RPCs (no trust required)
        self.public_rpcs = [
            "https://api.mainnet-beta.solana.com",
            "https://solana-api.projectserum.com", 
            "https://rpc.ankr.com/solana",
            "https://solana-mainnet.rpc.explorer.network"
        ]
    
    async def verify_cid_completely_standalone(self, cid: str) -> Dict[str, Any]:
        """
        Complete standalone verification of a CID.
        
        This is the ultimate test of endpointlessness.
        """
        verification = {
            "cid": cid,
            "verification_type": "complete_standalone",
            "timestamp": datetime.utcnow().isoformat(),
            "steps": {},
            "success": False,
            "trust_score": 0.0,
            "can_use_without_server": False
        }
        
        try:
            print(f"[Standalone] Starting complete verification of {cid}")
            
            # Step 1: Retrieve artifact from IPFS (no server dependency)
            artifact_data = await self._retrieve_from_ipfs_standalone(cid)
            if not artifact_data:
                verification["error"] = "Failed to retrieve artifact from IPFS"
                return verification
            
            verification["steps"]["ipfs_retrieval"] = {
                "success": True,
                "gateways_tried": len(self.ipfs_gateways),
                "content_size_bytes": len(json.dumps(artifact_data))
            }
            
            # Step 2: Verify content integrity (recompute SHA-256)
            content_verification = await self._verify_content_integrity_standalone(artifact_data)
            verification["steps"]["content_integrity"] = content_verification
            
            if not content_verification["verified"]:
                verification["error"] = "Content integrity verification failed"
                return verification
            
            # Step 3: Verify signatures independently (no original server)
            signature_verification = await self._verify_signatures_standalone(artifact_data)
            verification["steps"]["signature_verification"] = signature_verification
            
            # Step 4: Verify statistical consistency
            stats_verification = await self._verify_statistics_standalone(artifact_data)
            verification["steps"]["statistical_consistency"] = stats_verification
            
            # Step 5: Cross-validate with public RPCs
            cross_validation = await self._cross_validate_with_public_rpcs(artifact_data)
            verification["steps"]["cross_validation"] = cross_validation
            
            # Calculate overall trust score
            trust_score = self._calculate_standalone_trust_score(verification["steps"])
            verification["trust_score"] = trust_score
            
            # Determine if usable without server
            verification["success"] = trust_score >= 0.8
            verification["can_use_without_server"] = verification["success"]
            
            print(f"[Standalone] Verification complete: trust_score={trust_score:.2f}")
            
            return verification
            
        except Exception as e:
            verification["error"] = str(e)
            verification["success"] = False
            return verification
    
    async def _retrieve_from_ipfs_standalone(self, cid: str) -> Optional[Dict[str, Any]]:
        """Retrieve artifact from IPFS using public gateways only."""
        print(f"[Standalone] Retrieving {cid} from IPFS gateways...")
        
        for gateway in self.ipfs_gateways:
            try:
                url = f"{gateway}{cid}"
                print(f"[Standalone] Trying gateway: {gateway}")
                
                response = await self.client.get(url, timeout=30.0)
                
                if response.status_code == 200:
                    content = response.json()
                    print(f"[Standalone] Successfully retrieved from {gateway}")
                    return content
                else:
                    print(f"[Standalone] Gateway {gateway} returned {response.status_code}")
                    
            except Exception as e:
                print(f"[Standalone] Gateway {gateway} failed: {str(e)}")
                continue
        
        print(f"[Standalone] Failed to retrieve {cid} from all gateways")
        return None
    
    async def _verify_content_integrity_standalone(self, artifact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Verify content integrity by recomputing SHA-256."""
        print("[Standalone] Verifying content integrity...")
        
        verification = {
            "verified": False,
            "expected_hash": artifact_data.get("canonical_sha256"),
            "computed_hash": None,
            "matches": False
        }
        
        try:
            # Canonical JSON serialization
            canonical_json = json.dumps(artifact_data, sort_keys=True, separators=(',', ':'))
            
            # Compute SHA-256
            computed_hash = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
            verification["computed_hash"] = computed_hash
            
            # Compare
            verification["matches"] = computed_hash == verification["expected_hash"]
            verification["verified"] = verification["matches"]
            
            print(f"[Standalone] Hash verification: {verification['matches']}")
            print(f"[Standalone] Expected: {verification['expected_hash']}")
            print(f"[Standalone] Computed: {computed_hash}")
            
        except Exception as e:
            verification["error"] = str(e)
            print(f"[Standalone] Hash verification failed: {str(e)}")
        
        return verification
    
    async def _verify_signatures_standalone(self, artifact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Verify signatures using public RPCs only."""
        print("[Standalone] Verifying signatures with public RPCs...")
        
        verification = {
            "verified": False,
            "total_proofs": 0,
            "verified_signatures": 0,
            "verification_rate": 0.0,
            "rpc_results": {},
            "issues": []
        }
        
        try:
            # Extract verification proofs
            verification_proofs = artifact_data.get("verification", {}).get("verification_proofs", [])
            verification["total_proofs"] = len(verification_proofs)
            
            if not verification_proofs:
                verification["issues"].append("No verification proofs found")
                return verification
            
            # Sample signatures for verification (max 20 for performance)
            sample_proofs = verification_proofs[:20]
            
            for proof in sample_proofs:
                signature = proof.get("signature")
                if not signature:
                    continue
                
                print(f"[Standalone] Verifying signature: {signature[:16]}...")
                
                # Verify across multiple public RPCs
                rpc_results = await self._verify_signature_across_public_rpcs(signature)
                verification["rpc_results"][signature] = rpc_results
                
                # Count successful verifications
                if any(result.get("verified", False) for result in rpc_results.values()):
                    verification["verified_signatures"] += 1
            
            # Calculate verification rate
            if verification["total_proofs"] > 0:
                verification["verification_rate"] = verification["verified_signatures"] / min(len(sample_proofs), verification["total_proofs"])
            
            verification["verified"] = verification["verification_rate"] >= 0.7
            
            print(f"[Standalone] Signature verification: {verification['verification_rate']:.1%}")
            
        except Exception as e:
            verification["issues"].append(f"Signature verification failed: {str(e)}")
            print(f"[Standalone] Signature verification error: {str(e)}")
        
        return verification
    
    async def _verify_signature_across_public_rpcs(self, signature: str) -> Dict[str, Dict[str, Any]]:
        """Verify signature across multiple public RPC endpoints."""
        results = {}
        
        for rpc_url in self.public_rpcs:
            try:
                result = await self._verify_with_public_rpc(signature, rpc_url)
                results[rpc_url] = result
                
                if result.get("verified", False):
                    print(f"[Standalone] ✓ Verified via {rpc_url}")
                else:
                    print(f"[Standalone] ✗ Failed via {rpc_url}: {result.get('error', 'Unknown')}")
                    
            except Exception as e:
                results[rpc_url] = {
                    "verified": False,
                    "error": str(e),
                    "rpc": rpc_url
                }
        
        return results
    
    async def _verify_with_public_rpc(self, signature: str, rpc_url: str) -> Dict[str, Any]:
        """Verify signature with specific public RPC."""
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
                    "rpc": rpc_url,
                    "slot": result.get("slot"),
                    "blockTime": result.get("blockTime"),
                    "fee": meta.get("fee", 0),
                    "computeUnitsConsumed": meta.get("computeUnitsConsumed", 0),
                    "success": meta.get("err") is None
                }
            else:
                return {
                    "verified": False,
                    "rpc": rpc_url,
                    "error": data.get("error", "Not found")
                }
                
        except Exception as e:
            return {
                "verified": False,
                "rpc": rpc_url,
                "error": str(e)
            }
    
    async def _verify_statistics_standalone(self, artifact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Verify statistical consistency independently."""
        print("[Standalone] Verifying statistical consistency...")
        
        verification = {
            "verified": False,
            "checks": {},
            "issues": []
        }
        
        try:
            fee_stats = artifact_data.get("fees_statistics", {})
            
            if not fee_stats:
                verification["issues"].append("No fee statistics found")
                return verification
            
            # Check percentile ordering
            p50 = fee_stats.get("p50_micro_lamports_per_cu", 0)
            p75 = fee_stats.get("p75_micro_lamports_per_cu", 0)
            p90 = fee_stats.get("p90_micro_lamports_per_cu", 0)
            
            percentile_check = {
                "name": "percentile_ordering",
                "verified": p50 <= p75 <= p90,
                "values": {"p50": p50, "p75": p75, "p90": p90}
            }
            
            verification["checks"]["percentile_ordering"] = percentile_check
            
            # Check success rate bounds
            success_rate = fee_stats.get("success_rate", 0)
            success_rate_check = {
                "name": "success_rate_bounds",
                "verified": 0 <= success_rate <= 1,
                "value": success_rate
            }
            
            verification["checks"]["success_rate_bounds"] = success_rate_check
            
            # Check sample consistency
            sample_count = fee_stats.get("sample_count", 0)
            verified_samples = fee_stats.get("verified_samples", 0)
            
            sample_check = {
                "name": "sample_consistency", 
                "verified": verified_samples <= sample_count,
                "values": {"total": sample_count, "verified": verified_samples}
            }
            
            verification["checks"]["sample_consistency"] = sample_check
            
            # Check for non-zero fees (fixing the original issue)
            if p50 == 0 and p75 == 0 and p90 == 0:
                verification["issues"].append("All fee percentiles are zero - data extraction failed")
                verification["checks"]["non_zero_fees"] = {
                    "name": "non_zero_fees",
                    "verified": False,
                    "issue": "All fees are zero"
                }
            else:
                verification["checks"]["non_zero_fees"] = {
                    "name": "non_zero_fees",
                    "verified": True
                }
            
            # Overall verification
            all_checks_passed = all(check.get("verified", False) for check in verification["checks"].values())
            verification["verified"] = all_checks_passed and len(verification["issues"]) == 0
            
            print(f"[Standalone] Statistical verification: {verification['verified']}")
            
        except Exception as e:
            verification["issues"].append(f"Statistical verification failed: {str(e)}")
        
        return verification
    
    async def _cross_validate_with_public_rpcs(self, artifact_data: Dict[str, Any]) -> Dict[str, Any]:
        """Cross-validate artifact data with public RPCs."""
        print("[Standalone] Cross-validating with public RPCs...")
        
        validation = {
            "verified": False,
            "cross_validated_signatures": 0,
            "provider_disagreements": 0,
            "issues": []
        }
        
        try:
            # Get a few signatures to cross-validate
            verification_proofs = artifact_data.get("verification", {}).get("verification_proofs", [])
            sample_signatures = [p.get("signature") for p in verification_proofs[:5] if p.get("signature")]
            
            if not sample_signatures:
                validation["issues"].append("No signatures available for cross-validation")
                return validation
            
            for signature in sample_signatures:
                # Get original provider data
                original_proof = next((p for p in verification_proofs if p.get("signature") == signature), None)
                
                if not original_proof:
                    continue
                
                # Verify with public RPCs
                public_results = await self._verify_signature_across_public_rpcs(signature)
                
                # Check for disagreements
                public_verified = any(result.get("verified", False) for result in public_results.values())
                original_verified = original_proof.get("verified", False)
                
                if public_verified != original_verified:
                    validation["provider_disagreements"] += 1
                    validation["issues"].append(
                        f"Provider disagreement for {signature[:16]}: original={original_verified}, public={public_verified}"
                    )
                else:
                    validation["cross_validated_signatures"] += 1
            
            validation["verified"] = validation["provider_disagreements"] == 0
            
            print(f"[Standalone] Cross-validation: {validation['cross_validated_signatures']} verified, {validation['provider_disagreements']} disagreements")
            
        except Exception as e:
            validation["issues"].append(f"Cross-validation failed: {str(e)}")
        
        return validation
    
    def _calculate_standalone_trust_score(self, steps: Dict[str, Any]) -> float:
        """Calculate trust score for standalone verification."""
        weights = {
            "ipfs_retrieval": 0.2,
            "content_integrity": 0.3,
            "signature_verification": 0.3,
            "statistical_consistency": 0.1,
            "cross_validation": 0.1
        }
        
        total_score = 0.0
        
        for step_name, weight in weights.items():
            step = steps.get(step_name, {})
            
            if step.get("verified", False):
                # Base score for passing
                score = 1.0
                
                # Penalize for issues
                issues = step.get("issues", [])
                if issues:
                    score -= len(issues) * 0.2
                
                # Special penalties for critical failures
                if step_name == "statistical_consistency":
                    checks = step.get("checks", {})
                    non_zero_fees = checks.get("non_zero_fees", {})
                    if not non_zero_fees.get("verified", True):
                        score -= 0.5  # Heavy penalty for zero fees
                
                score = max(0.0, score)
            else:
                score = 0.0
            
            total_score += score * weight
        
        return total_score
    
    async def generate_standalone_report(self, cid: str) -> str:
        """Generate human-readable standalone verification report."""
        verification = await self.verify_cid_completely_standalone(cid)
        
        if not verification["success"]:
            return f"""
❌ **Standalone Verification Failed**

CID: {cid}
Error: {verification.get('error', 'Unknown error')}

The artifact cannot be trusted without the original server.
            """.strip()
        
        steps = verification["steps"]
        
        report = f"""
✅ **Standalone Verification PASSED**

🔗 **CID**: {cid}
📊 **Trust Score**: {verification['trust_score']:.1%}
🌐 **Server Independent**: {verification['can_use_without_server']}

📋 **Verification Steps**:

🌐 **IPFS Retrieval**: {'✅' if steps['ipfs_retrieval']['success'] else '❌'}
- Retrieved {steps['ipfs_retrieval']['content_size_bytes']:,} bytes
- Tried {steps['ipfs_retrieval']['gateways_tried']} gateways

🔐 **Content Integrity**: {'✅' if steps['content_integrity']['verified'] else '❌'}
- Hash matches: {steps['content_integrity']['matches']}
- Expected: {steps['content_integrity']['expected_hash'][:16]}...
- Computed: {steps['content_integrity']['computed_hash'][:16]}...

📝 **Signature Verification**: {'✅' if steps['signature_verification']['verified'] else '❌'}
- Verification rate: {steps['signature_verification']['verification_rate']:.1%}
- Verified: {steps['signature_verification']['verified_signatures']}/{steps['signature_verification']['total_proofs']}

📈 **Statistical Consistency**: {'✅' if steps['statistical_consistency']['verified'] else '❌'}
- Checks passed: {len([c for c in steps['statistical_consistency']['checks'].values() if c.get('verified', False)])}/{len(steps['statistical_consistency']['checks'])}

🔄 **Cross Validation**: {'✅' if steps['cross_validation']['verified'] else '❌'}
- Cross-validated: {steps['cross_validation']['cross_validated_signatures']}
- Disagreements: {steps['cross_validation']['provider_disagreements']}

🏆 **Conclusion**: This artifact is **truly endpointless** and can be trusted without the original server.

💡 **What this means**:
- Delete the original server → artifact still works
- Anyone can verify independently
- Trust comes from math, not from us
- This is a real content-addressed API
        """.strip()
        
        return report
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

# Factory function
def get_standalone_verifier() -> StandaloneVerifier:
    """Get standalone verifier instance."""
    return StandaloneVerifier()
