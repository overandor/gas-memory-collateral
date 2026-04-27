"""
Strict IPFS Storage

Enforces strict IPFS storage requirements:
- Fail if pin fails
- Verify hash match before returning CID
- No silent fallbacks
"""
import hashlib
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
import httpx

from app.services.persistent_storage import StorageResult, StorageReferences

class StrictIPFSStorage:
    """
    Strict IPFS storage that enforces quality and correctness.
    
    No more silent fallbacks. No more fake CIDs.
    Either it works correctly, or it fails loudly.
    """
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Pinata configuration
        self.pinata_api_key = None
        self.pinata_secret_key = None
        self.pinata_jwt = None
        
        # Load from environment
        self._load_config()
    
    def _load_config(self):
        """Load IPFS configuration from environment."""
        import os
        
        self.pinata_api_key = os.getenv("PINATA_API_KEY")
        self.pinata_secret_key = os.getenv("PINATA_SECRET_KEY")
        self.pinata_jwt = os.getenv("PINATA_JWT")
        
        if not all([self.pinata_api_key, self.pinata_secret_key, self.pinata_jwt]):
            raise Exception("Strict IPFS requires PINATA_API_KEY, PINATA_SECRET_KEY, and PINATA_JWT")
    
    async def store_artifact_strict(
        self,
        content_bytes: bytes,
        filename: str,
        expected_hash: Optional[str] = None
    ) -> StorageResult:
        """
        Store artifact with strict enforcement.
        
        Args:
            content_bytes: Content to store
            filename: Filename for the artifact
            expected_hash: Expected SHA-256 hash (optional)
            
        Returns:
            StorageResult with strict validation
            
        Raises:
            Exception: If any step fails
        """
        try:
            # Step 1: Calculate content hash
            content_hash = self._calculate_content_hash(content_bytes)
            
            # Step 2: Verify hash matches expected (if provided)
            if expected_hash and content_hash != expected_hash:
                raise Exception(
                    f"Content hash mismatch. Expected: {expected_hash}, Got: {content_hash}"
                )
            
            # Step 3: Upload to Pinata with strict validation
            pinata_result = await self._upload_to_pinata_strict(content_bytes, filename)
            
            # Step 4: Verify CID resolves to correct content
            verified_cid = await self._verify_cid_content(pinata_result["cid"], content_hash)
            
            # Step 5: Build strict storage result
            storage_result = StorageResult(
                success=True,
                identifier=verified_cid,
                gateway_url=f"https://gateway.pinata.cloud/ipfs/{verified_cid}",
                network="ipfs",
                metadata={
                    "strict_storage": True,
                    "content_hash": content_hash,
                    "pinata_cid": pinata_result["cid"],
                    "verified_cid": verified_cid,
                    "filename": filename,
                    "content_size_bytes": len(content_bytes),
                    "stored_at": datetime.utcnow().isoformat(),
                    "verification_passed": True
                }
            )
            
            return storage_result
            
        except Exception as e:
            # Return failed result with error details
            return StorageResult(
                success=False,
                identifier=None,
                gateway_url=None,
                network="ipfs",
                error=str(e),
                metadata={
                    "strict_storage": True,
                    "failed_at": datetime.utcnow().isoformat(),
                    "error_type": "storage_failure"
                }
            )
    
    def _calculate_content_hash(self, content_bytes: bytes) -> str:
        """Calculate SHA-256 hash of content."""
        return hashlib.sha256(content_bytes).hexdigest()
    
    async def _upload_to_pinata_strict(self, content_bytes: bytes, filename: str) -> Dict[str, Any]:
        """Upload to Pinata with strict validation."""
        if not all([self.pinata_api_key, self.pinata_secret_key, self.pinata_jwt]):
            raise Exception("Pinata credentials not configured")
        
        # Prepare file upload
        files = {
            'file': (filename, content_bytes, 'application/json')
        }
        
        # Pinata metadata
        pinata_metadata = {
            "name": f"gas-memory-{filename}",
            "keyvalues": {
                "type": "gas-memory-artifact",
                "created_at": datetime.utcnow().isoformat(),
                "strict_storage": "true"
            }
        }
        
        headers = {
            'pinata_api_key': self.pinata_api_key,
            'pinata_secret_key': self.pinata_secret_key,
            'Authorization': f'Bearer {self.pinata_jwt}'
        }
        
        try:
            # Upload to Pinata
            response = await self.client.post(
                "https://api.pinata.cloud/pinning/pinFileToIPFS",
                files=files,
                data={'pinataMetadata': json.dumps(pinata_metadata)},
                headers=headers
            )
            
            if response.status_code != 200:
                raise Exception(f"Pinata upload failed: {response.status_code} - {response.text}")
            
            result = response.json()
            
            if 'IpfsHash' not in result:
                raise Exception("Pinata response missing IpfsHash")
            
            # Verify Pinata response structure
            cid = result['IpfsHash']
            if not cid or len(cid) < 46:  # CID should be at least 46 chars
                raise Exception(f"Invalid CID from Pinata: {cid}")
            
            return {
                "cid": cid,
                "size": result.get("PinSize", len(content_bytes)),
                "timestamp": result.get("timestamp", datetime.utcnow().isoformat())
            }
            
        except httpx.RequestError as e:
            raise Exception(f"Pinata request failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Pinata upload error: {str(e)}")
    
    async def _verify_cid_content(self, cid: str, expected_hash: str) -> str:
        """Verify CID resolves to content with matching hash."""
        try:
            # Try multiple gateways for verification
            gateways = [
                f"https://gateway.pinata.cloud/ipfs/{cid}",
                f"https://ipfs.io/ipfs/{cid}",
                f"https://cloudflare-ipfs.com/ipfs/{cid}"
            ]
            
            content_hash = None
            verified_gateway = None
            
            for gateway_url in gateways:
                try:
                    response = await self.client.get(gateway_url, timeout=30.0)
                    
                    if response.status_code == 200:
                        content = response.content
                        calculated_hash = hashlib.sha256(content).hexdigest()
                        
                        if calculated_hash == expected_hash:
                            content_hash = calculated_hash
                            verified_gateway = gateway_url
                            break
                        else:
                            print(f"[StrictIPFS] Hash mismatch on {gateway_url}")
                            
                except Exception as e:
                    print(f"[StrictIPFS] Gateway {gateway_url} failed: {str(e)}")
                    continue
            
            if not content_hash:
                raise Exception(f"Failed to verify CID {cid} on any gateway")
            
            if content_hash != expected_hash:
                raise Exception(
                    f"Content hash mismatch for CID {cid}. "
                    f"Expected: {expected_hash}, Got: {content_hash}"
                )
            
            print(f"[StrictIPFS] Verified CID {cid} on {verified_gateway}")
            return cid
            
        except Exception as e:
            raise Exception(f"CID verification failed: {str(e)}")
    
    async def retrieve_artifact_strict(self, cid: str) -> bytes:
        """Retrieve artifact with strict verification."""
        try:
            # Try gateways in order
            gateways = [
                f"https://gateway.pinata.cloud/ipfs/{cid}",
                f"https://ipfs.io/ipfs/{cid}",
                f"https://cloudflare-ipfs.com/ipfs/{cid}"
            ]
            
            for gateway_url in gateways:
                try:
                    response = await self.client.get(gateway_url, timeout=30.0)
                    
                    if response.status_code == 200:
                        content = response.content
                        
                        # Verify content is not empty
                        if len(content) == 0:
                            raise Exception("Empty content retrieved")
                        
                        # Verify content is valid JSON (for our artifacts)
                        try:
                            json.loads(content.decode('utf-8'))
                        except json.JSONDecodeError:
                            raise Exception("Retrieved content is not valid JSON")
                        
                        print(f"[StrictIPFS] Retrieved {len(content)} bytes from {gateway_url}")
                        return content
                        
                except Exception as e:
                    print(f"[StrictIPFS] Gateway {gateway_url} failed: {str(e)}")
                    continue
            
            raise Exception(f"Failed to retrieve CID {cid} from any gateway")
            
        except Exception as e:
            raise Exception(f"Strict retrieval failed: {str(e)}")
    
    async def verify_cid_integrity(self, cid: str, expected_hash: str) -> Dict[str, Any]:
        """Verify CID integrity against expected hash."""
        try:
            content = await self.retrieve_artifact_strict(cid)
            calculated_hash = hashlib.sha256(content).hexdigest()
            
            verification_result = {
                "cid": cid,
                "expected_hash": expected_hash,
                "calculated_hash": calculated_hash,
                "matches": calculated_hash == expected_hash,
                "content_size": len(content),
                "verified_at": datetime.utcnow().isoformat()
            }
            
            if not verification_result["matches"]:
                verification_result["error"] = "Hash mismatch detected"
            
            return verification_result
            
        except Exception as e:
            return {
                "cid": cid,
                "expected_hash": expected_hash,
                "matches": False,
                "error": str(e),
                "verified_at": datetime.utcnow().isoformat()
            }
    
    async def get_storage_health_strict(self) -> Dict[str, Any]:
        """Get strict storage health status."""
        health = {
            "strict_storage": True,
            "pinata_configured": all([self.pinata_api_key, self.pinata_secret_key, self.pinata_jwt]),
            "test_cid": None,
            "test_result": None,
            "health_check_at": datetime.utcnow().isoformat()
        }
        
        if health["pinata_configured"]:
            try:
                # Test with a small payload
                test_content = json.dumps({
                    "test": True,
                    "timestamp": datetime.utcnow().isoformat(),
                    "strict_storage": True
                }).encode('utf-8')
                
                test_result = await self.store_artifact_strict(test_content, "health-check.json")
                
                if test_result.success:
                    health["test_cid"] = test_result.identifier
                    health["test_result"] = "passed"
                    
                    # Clean up test
                    try:
                        await self._delete_pinata_pin(test_result.identifier)
                    except:
                        pass  # Cleanup failure doesn't affect health check
                else:
                    health["test_result"] = f"failed: {test_result.error}"
                    
            except Exception as e:
                health["test_result"] = f"error: {str(e)}"
        else:
            health["test_result"] = "skipped - not configured"
        
        return health
    
    async def _delete_pinata_pin(self, cid: str):
        """Delete a pin from Pinata (for cleanup)."""
        if not all([self.pinata_api_key, self.pinata_secret_key, self.pinata_jwt]):
            return
        
        headers = {
            'pinata_api_key': self.pinata_api_key,
            'pinata_secret_key': self.pinata_secret_key,
            'Authorization': f'Bearer {self.pinata_jwt}'
        }
        
        try:
            response = await self.client.delete(
                f"https://api.pinata.cloud/pinning/unpin/{cid}",
                headers=headers
            )
            
            if response.status_code == 200:
                print(f"[StrictIPFS] Cleaned up test pin {cid}")
                
        except Exception as e:
            print(f"[StrictIPFS] Failed to cleanup test pin {cid}: {str(e)}")
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

# Factory function
def get_strict_ipfs_storage() -> StrictIPFSStorage:
    """Get strict IPFS storage instance."""
    return StrictIPFSStorage()
