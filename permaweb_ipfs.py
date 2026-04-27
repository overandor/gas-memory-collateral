"""
Permaweb IPFS Integration
Connects Gas Memory to the permaweb naming system
"""
import json
import hashlib
from typing import Optional, Dict, Any
from datetime import datetime
import httpx

from app.models.schemas import GasMemoryBundle, IPFSStoreResponse
from app.utils.config import settings


class PermawebIPFSClient:
    """Client for permaweb naming + IPFS storage."""
    
    def __init__(self):
        # Use permaweb local server (port 9000) or Docker IPFS (port 5001)
        self.permaweb_api = "http://localhost:9000"  # Local Python server
        self.ipfs_api = settings.IPFS_API_URL  # Docker: http://localhost:5001
        self.gateway_url = settings.IPFS_GATEWAY
        self.timeout = 30.0
        self.client = httpx.AsyncClient(timeout=self.timeout)
    
    async def check_health(self) -> Dict[str, Any]:
        """Check connection to IPFS and permaweb naming."""
        results = {
            "permaweb_naming": False,
            "ipfs_local": False,
            "ipfs_docker": False,
            "overall": False
        }
        
        # Check permaweb naming
        try:
            r = await self.client.get(f"{self.permaweb_api}/health", timeout=5.0)
            results["permaweb_naming"] = r.status_code == 200
        except Exception:
            pass
        
        # Check Docker IPFS
        try:
            r = await self.client.post(f"{self.ipfs_api}/api/v0/id", timeout=5.0)
            results["ipfs_docker"] = r.status_code == 200
        except Exception:
            pass
        
        # Check local IPFS (permaweb server)
        try:
            r = await self.client.get(f"{self.permaweb_api}/stats", timeout=5.0)
            if r.status_code == 200:
                data = r.json()
                results["ipfs_local"] = data.get("ipfs", {}).get("objects", 0) >= 0
        except Exception:
            pass
        
        results["overall"] = results["permaweb_naming"] or results["ipfs_docker"] or results["ipfs_local"]
        return results
    
    async def store_bundle(
        self,
        bundle: GasMemoryBundle,
        pin: bool = True,
        name: Optional[str] = None
    ) -> IPFSStoreResponse:
        """
        Store bundle on IPFS and optionally register a name.
        
        Args:
            bundle: The GasMemoryBundle to store
            pin: Whether to pin the content
            name: Optional human-readable name (e.g., "drift_fees_2024")
        """
        # Serialize to canonical JSON
        bundle_json = self._canonical_json(bundle)
        bundle_bytes = bundle_json.encode('utf-8')
        bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()
        
        # Try permaweb local server first
        try:
            cid = await self._store_to_permaweb(bundle_bytes, pin)
            is_real = cid.startswith("Qm")
            
            # Register name if provided
            if name and is_real:
                await self._register_name(name, cid, bundle)
            
            return IPFSStoreResponse(
                ok=True,
                cid=f"ipfs://{cid}" if is_real else cid,
                ipfs_gateway_url=f"{self.permaweb_api}/ipfs/{cid}",
                bundle_hash=bundle_hash,
                pinned=pin,
                size_bytes=len(bundle_bytes),
                provider="permaweb-local",
                storage_type="local_ipfs" if is_real else "memory_store",
                is_real_ipfs=is_real
            )
            
        except Exception:
            # Fallback to local hash
            local_cid = f"local://{bundle_hash}"
            return IPFSStoreResponse(
                ok=False,
                cid=local_cid,
                ipfs_gateway_url=None,
                bundle_hash=bundle_hash,
                pinned=False,
                size_bytes=len(bundle_bytes),
                provider="fallback",
                storage_type="local_dev_store",
                is_real_ipfs=False,
                not_ipfs=True
            )
    
    async def _store_to_permaweb(self, data: bytes, pin: bool = True) -> str:
        """Store data to permaweb local IPFS."""
        files = {'file': ('bundle.json', data, 'application/json')}
        
        r = await self.client.post(
            f"{self.permaweb_api}/ipfs/add",
            files=files
        )
        r.raise_for_status()
        result = r.json()
        
        return result.get('Hash', '')
    
    async def _register_name(
        self,
        name: str,
        cid: str,
        bundle: GasMemoryBundle
    ) -> bool:
        """Register a human-readable name for the CID."""
        try:
            data = {
                "name": name.lower().replace(" ", "_"),
                "cid": cid,
                "content_type": "application/json",
                "description": f"Gas Memory: {bundle.scope.tx_family} on {bundle.chain.name}",
                "ttl": 3600
            }
            
            r = await self.client.post(
                f"{self.permaweb_api}/names",
                json=data
            )
            return r.status_code in (200, 201)
        except Exception:
            return False
    
    async def resolve_name(self, name: str) -> Optional[str]:
        """Resolve a name to CID."""
        try:
            r = await self.client.get(f"{self.permaweb_api}/resolve/{name}")
            if r.status_code == 200:
                data = r.json()
                return data.get('cid')
        except Exception:
            pass
        return None
    
    async def retrieve_bundle(self, cid: str) -> Optional[GasMemoryBundle]:
        """Retrieve a bundle by CID."""
        try:
            # Remove ipfs:// prefix if present
            cid = cid.replace("ipfs://", "")
            
            r = await self.client.get(f"{self.permaweb_api}/ipfs/{cid}")
            if r.status_code == 200:
                data = r.json()
                return GasMemoryBundle(**data)
        except Exception:
            pass
        return None
    
    def _canonical_json(self, obj: Any) -> str:
        """Create canonical JSON for consistent hashing."""
        def serialize(o):
            if isinstance(o, datetime):
                return o.isoformat()
            elif hasattr(o, 'model_dump'):
                return serialize(o.model_dump())
            elif isinstance(o, dict):
                return {k: serialize(v) for k, v in sorted(o.items())}
            elif isinstance(o, list):
                return [serialize(i) for i in o]
            return o
        
        try:
            return json.dumps(serialize(obj), separators=(',', ':'), ensure_ascii=False)
        except Exception:
            # Handle exception
            return ""
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage stats."""
        try:
            r = await self.client.get(f"{self.permaweb_api}/stats")
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return {}
    
    async def close(self):
        await self.client.aclose()


# Global instance
_permaweb_client: Optional[PermawebIPFSClient] = None


async def get_permaweb_ipfs() -> PermawebIPFSClient:
    """Get or create permaweb IPFS client."""
    global _permaweb_client
    if _permaweb_client is None:
        _permaweb_client = PermawebIPFSClient()
    return _permaweb_client
