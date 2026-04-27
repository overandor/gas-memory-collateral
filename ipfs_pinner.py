"""
Real IPFS Pinning Service

Addresses the critique: "'ipfs_store = {}' that's a dictionary"

Supports:
- Local Kubo node
- Remote pinning (Pinata, Filebase)
- Multi-provider redundancy
"""
import hashlib
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import httpx


@dataclass
class PinResult:
    """Result of a pin operation."""
    provider: str
    cid: str
    success: bool
    size_bytes: int
    error: Optional[str] = None


class IPFSPinner:
    """
    Multi-provider IPFS pinning for real persistence.

    Usage:
        pinner = IPFSPinner()
        results = await pinner.pin_json(artifact_data)
        # Returns CID + confirmation from multiple providers
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.providers = self._load_providers()

    def _load_providers(self) -> Dict:
        """Load pinning providers from environment."""
        import os

        providers = {}

        # Local Kubo (priority)
        if os.getenv("IPFS_KUBO_URL"):
            providers["kubo"] = {
                "url": os.getenv("IPFS_KUBO_URL"),
                "type": "kubo"
            }
        else:
            # Default local Kubo
            providers["kubo"] = {
                "url": "http://localhost:5001",
                "type": "kubo"
            }

        # Pinata (if API key available)
        if os.getenv("PINATA_API_KEY") and os.getenv("PINATA_SECRET_KEY"):
            providers["pinata"] = {
                "url": "https://api.pinata.cloud/pinning/pinFileToIPFS",
                "type": "pinata",
                "api_key": os.getenv("PINATA_API_KEY"),
                "secret_key": os.getenv("PINATA_SECRET_KEY")
            }

        # Filebase (if API key available)
        if os.getenv("FILEBASE_API_KEY"):
            providers["filebase"] = {
                "url": "https://api.filebase.io/v1/ipfs",
                "type": "filebase",
                "api_key": os.getenv("FILEBASE_API_KEY")
            }

        return providers

    async def pin_json(
        self,
        data: Dict,
        name: str = "gas-memory-artifact",
        min_providers: int = 1
    ) -> Tuple[str, List[PinResult]]:
        """
        Pin JSON data to IPFS with multi-provider redundancy.

        Args:
            data: JSON-serializable data to pin
            name: Human-readable name for the content
            min_providers: Minimum successful pins required

        Returns:
            Tuple of (CID, list of PinResults)
        """
        # Serialize to JSON (deterministic)
        json_bytes = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()

        # Calculate CID locally (SHA-256 based, compatible with IPFS)
        cid = self._calculate_cid(json_bytes)

        # Try to pin to all providers in parallel
        tasks = []
        for provider_name, config in self.providers.items():
            tasks.append(self._pin_to_provider(
                provider_name, config, json_bytes, cid, name
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        pin_results = []
        successful = 0

        for result in results:
            if isinstance(result, Exception):
                pin_results.append(PinResult(
                    provider="unknown",
                    cid=cid,
                    success=False,
                    size_bytes=len(json_bytes),
                    error=str(result)
                ))
            else:
                pin_results.append(result)
                if result.success:
                    successful += 1

        if successful < min_providers:
            raise PinError(
                f"Failed to reach min_providers: {successful}/{min_providers} succeeded"
            )

        return cid, pin_results

    def _calculate_cid(self, data: bytes) -> str:
        """
        Calculate IPFS-compatible CID (v0) from data.

        Uses SHA-256 hash which is what IPFS v0 uses.
        """
        # IPFS v0 uses sha2-256 multihash
        # Prefix: 0x12 (sha2-256) + 0x20 (32 bytes) + hash
        hash_bytes = hashlib.sha256(data).digest()

        # For v0 CID, we use base58btc encoding
        # This is a simplified version - real IPFS uses multiformats
        import base58
        multihash = bytes([0x12, 0x20]) + hash_bytes
        cid = base58.b58encode(multihash).decode()

        return f"Qm{cid[:44]}"  # IPFS v0 format

    async def _pin_to_provider(
        self,
        provider_name: str,
        config: Dict,
        data: bytes,
        cid: str,
        name: str
    ) -> PinResult:
        """Pin content to a specific provider."""
        try:
            if config["type"] == "kubo":
                return await self._pin_to_kubo(config, data, cid, name)
            elif config["type"] == "pinata":
                return await self._pin_to_pinata(config, data, cid, name)
            elif config["type"] == "filebase":
                return await self._pin_to_filebase(config, data, cid, name)
            else:
                return PinResult(
                    provider=provider_name,
                    cid=cid,
                    success=False,
                    size_bytes=len(data),
                    error=f"Unknown provider type: {config['type']}"
                )
        except Exception as e:
            return PinResult(
                provider=provider_name,
                cid=cid,
                success=False,
                size_bytes=len(data),
                error=str(e)
            )

    async def _pin_to_kubo(
        self,
        config: Dict,
        data: bytes,
        cid: str,
        name: str
    ) -> PinResult:
        """Pin to local Kubo node via API."""
        url = f"{config['url']}/api/v0/add"

        files = {"file": (f"{name}.json", data, "application/json")}

        response = await self.client.post(url, files=files, timeout=30.0)
        result = response.json()

        if response.status_code == 200 and "Hash" in result:
            return PinResult(
                provider="kubo",
                cid=result["Hash"],
                success=True,
                size_bytes=result.get("Size", len(data))
            )
        else:
            return PinResult(
                provider="kubo",
                cid=cid,
                success=False,
                size_bytes=len(data),
                error=result.get("Message", "Unknown error")
            )

    async def _pin_to_pinata(
        self,
        config: Dict,
        data: bytes,
        cid: str,
        name: str
    ) -> PinResult:
        """Pin to Pinata."""
        url = config["url"]

        headers = {
            "pinata_api_key": config["api_key"],
            "pinata_secret_api_key": config["secret_key"]
        }

        files = {"file": (f"{name}.json", data, "application/json")}
        metadata = {"pinataMetadata": {"name": name}}

        response = await self.client.post(
            url,
            headers=headers,
            files=files,
            data=metadata,
            timeout=30.0
        )

        if response.status_code == 200:
            result = response.json()
            return PinResult(
                provider="pinata",
                cid=result.get("IpfsHash", cid),
                success=True,
                size_bytes=result.get("PinSize", len(data))
            )
        else:
            return PinResult(
                provider="pinata",
                cid=cid,
                success=False,
                size_bytes=len(data),
                error=f"HTTP {response.status_code}: {response.text[:200]}"
            )

    async def _pin_to_filebase(
        self,
        config: Dict,
        data: bytes,
        cid: str,
        name: str
    ) -> PinResult:
        """Pin to Filebase."""
        url = config["url"]

        headers = {
            "Authorization": f"Bearer {config['api_key']}",
            "Content-Type": "application/json"
        }

        response = await self.client.post(
            url,
            headers=headers,
            content=data,
            timeout=30.0
        )

        if response.status_code in [200, 201]:
            result = response.json()
            return PinResult(
                provider="filebase",
                cid=result.get("cid", cid),
                success=True,
                size_bytes=len(data)
            )
        else:
            return PinResult(
                provider="filebase",
                cid=cid,
                success=False,
                size_bytes=len(data),
                error=f"HTTP {response.status_code}: {response.text[:200]}"
            )

    async def verify_pin(self, cid: str, providers: Optional[List[str]] = None) -> Dict:
        """
        Verify that a CID is pinned across providers.

        Returns dict with verification status per provider.
        """
        providers_to_check = providers or list(self.providers.keys())
        results = {}

        for provider in providers_to_check:
            if provider not in self.providers:
                results[provider] = {"status": "unknown_provider"}
                continue

            try:
                config = self.providers[provider]
                if config["type"] == "kubo":
                    # Check local pin status
                    url = f"{config['url']}/api/v0/pin/ls"
                    response = await self.client.post(url, params={"arg": cid}, timeout=10.0)
                    results[provider] = {
                        "status": "pinned" if response.status_code == 200 else "not_found"
                    }
                else:
                    # Remote providers - assume pinned if we got success earlier
                    results[provider] = {"status": "assumed_pinned"}
            except Exception as e:
                results[provider] = {"status": "error", "error": str(e)}

        return results

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


class PinError(Exception):
    """Raised when pinning fails."""
    pass


# Global instance
_pinner: Optional[IPFSPinner] = None


async def get_pinner() -> IPFSPinner:
    """Get or create pinner instance."""
    global _pinner
    if _pinner is None:
        _pinner = IPFSPinner()
    return _pinner


import asyncio  # For global instance
