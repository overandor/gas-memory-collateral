"""
Jito Bundle Service
Handles atomic bundle submission via Jito block engine.
Bundles ensure ordered execution and MEV-aware submission.
"""
import base64
import json
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
import httpx
import time


@dataclass
class BundleResult:
    """Result of bundle submission."""
    success: bool
    bundle_id: Optional[str] = None
    transactions: List[str] = None
    landed: bool = False
    confirmed: bool = False
    slot: Optional[int] = None
    error: Optional[str] = None
    raw_response: Dict = None


@dataclass
class TipEstimation:
    """Estimated Jito tip for bundle inclusion."""
    min_tip_lamports: int
    recommended_tip_lamports: int
    aggressive_tip_lamports: int
    estimated_inclusion_probability: float


class JitoService:
    """Service for submitting transactions via Jito bundles."""
    
    # Jito endpoints
    MAINNET_BUNDLE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
    MAINNET_TX_URL = "https://mainnet.block-engine.jito.wtf/api/v1/transactions"
    
    # Tip account (Jito tip receiver)
    TIP_ACCOUNT = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqGNyQ7c4v6YJ7p8"
    
    # Default tip amounts (lamports)
    DEFAULT_TIP = 1_000
    MIN_TIP = 100
    AGGRESSIVE_TIP = 10_000
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.bundle_url = self.MAINNET_BUNDLE_URL
        self.tx_url = self.MAINNET_TX_URL
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers=headers
            )
        return self._client
    
    def estimate_tip(
        self,
        priority: str = "normal",  # "min", "normal", "aggressive"
        compute_units: int = 200_000
    ) -> TipEstimation:
        """Estimate appropriate Jito tip for bundle inclusion."""
        base_min = self.MIN_TIP
        base_recommended = self.DEFAULT_TIP
        base_aggressive = self.AGGRESSIVE_TIP
        
        # Scale with compute units (higher compute = higher tip)
        cu_multiplier = compute_units / 200_000
        
        min_tip = int(base_min * cu_multiplier)
        recommended = int(base_recommended * cu_multiplier)
        aggressive = int(base_aggressive * cu_multiplier)
        
        # Priority adjustment
        if priority == "min":
            recommended = min_tip
            aggressive = int(base_recommended * cu_multiplier)
        elif priority == "aggressive":
            min_tip = recommended
            recommended = aggressive
            aggressive = int(aggressive * 2)
        
        # Estimate inclusion probability
        if priority == "aggressive":
            prob = 0.95
        elif priority == "normal":
            prob = 0.75
        else:
            prob = 0.50
        
        return TipEstimation(
            min_tip_lamports=min_tip,
            recommended_tip_lamports=recommended,
            aggressive_tip_lamports=aggressive,
            estimated_inclusion_probability=prob
        )
    
    async def send_bundle(
        self,
        transactions: List[str],  # base64-encoded transactions
        tip_lamports: int = None,
        skip_simulation: bool = False
    ) -> BundleResult:
        """
        Submit bundle to Jito block engine.
        
        Args:
            transactions: List of base64-encoded transactions
            tip_lamports: Optional tip amount
            skip_simulation: Skip pre-flight simulation
        
        Returns:
            BundleResult with bundle ID and status
        """
        try:
            client = await self._get_client()
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendBundle",
                "params": [
                    transactions,
                    {
                        "skipSimulate": skip_simulation
                    }
                ]
            }
            
            response = await client.post(
                self.bundle_url,
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                return BundleResult(
                    success=False,
                    error=str(data["error"]),
                    raw_response=data
                )
            
            result = data.get("result", {})
            bundle_id = result.get("bundleId")
            
            return BundleResult(
                success=True,
                bundle_id=bundle_id,
                transactions=transactions,
                raw_response=data
            )
            
        except Exception as e:
            return BundleResult(
                success=False,
                error=f"Bundle submission failed: {str(e)}"
            )
    
    async def get_bundle_status(
        self,
        bundle_id: str,
        max_wait_seconds: int = 60
    ) -> BundleResult:
        """
        Poll for bundle confirmation status.
        
        Args:
            bundle_id: Bundle ID from send_bundle
            max_wait_seconds: Maximum time to wait for confirmation
        
        Returns:
            BundleResult with landed status
        """
        # Jito doesn't have a direct status endpoint
        # We need to check if transactions landed via RPC
        # For now, return pending status
        
        return BundleResult(
            success=True,
            bundle_id=bundle_id,
            landed=False,  # Would need RPC check
            confirmed=False
        )
    
    async def send_transaction(
        self,
        transaction: str,  # base64-encoded
        skip_preflight: bool = False
    ) -> Dict:
        """
        Send single transaction via Jito (non-bundle fallback).
        
        Useful for testing or when bundle isn't needed.
        """
        try:
            client = await self._get_client()
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    transaction,
                    {
                        "skipPreflight": skip_preflight,
                        "encoding": "base64"
                    }
                ]
            }
            
            response = await client.post(
                self.tx_url,
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            return {
                "error": f"Transaction submission failed: {str(e)}"
            }
    
    async def close(self):
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Singleton
_jito: Optional[JitoService] = None


def get_jito_service(api_key: Optional[str] = None) -> JitoService:
    """Get Jito service singleton."""
    global _jito
    if _jito is None:
        _jito = JitoService(api_key)
    return _jito
