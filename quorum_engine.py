"""
Quorum Engine

Real consensus through diversity, not just multiplicity.
5 RPCs with median + deviation filter, provider fingerprinting.
"""
import asyncio
import statistics
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import httpx
import hashlib

@dataclass
class RPCProvider:
    """RPC provider configuration with fingerprinting."""
    name: str
    url: str
    provider_type: str  # "public", "helius", "triton", "self_hosted", "alchemy"
    region: str  # "us-east", "us-west", "eu", "asia"
    priority: int  # 1-5, lower = higher priority
    
    def fingerprint(self) -> str:
        """Generate provider fingerprint for diversity tracking."""
        fingerprint_data = f"{self.name}:{self.provider_type}:{self.region}"
        return hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]

@dataclass
class QuorumResult:
    """Result from quorum consensus."""
    consensus_value: Any
    provider_responses: Dict[str, Any]
    median_value: float
    deviation: float
    consensus_strength: float  # 0.0 to 1.0
    outlier_count: int
    provider_fingerprints: List[str]
    quorum_met: bool

class QuorumEngine:
    """
    Quorum engine for real consensus through provider diversity.
    
    Instead of "multiple RPCs = truth", we use:
    - Diverse provider types (public, commercial, self-hosted)
    - Geographic distribution
    - Median filtering + deviation detection
    - Provider fingerprinting for audit trails
    """
    
    def __init__(self):
        self.providers = self._initialize_diverse_providers()
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Quorum requirements
        self.min_providers = 3
        self.max_deviation_percent = 20.0  # Max 20% deviation from median
        self.consensus_threshold = 0.6  # 60% of providers must agree
    
    def _initialize_diverse_providers(self) -> List[RPCProvider]:
        """Initialize diverse set of RPC providers."""
        return [
            # Public RPCs (baseline)
            RPCProvider(
                name="solana-public",
                url="https://api.mainnet-beta.solana.com",
                provider_type="public",
                region="us-east",
                priority=3
            ),
            RPCProvider(
                name="project-serum",
                url="https://solana-api.projectserum.com",
                provider_type="public",
                region="us-west",
                priority=3
            ),
            
            # Commercial providers (different backends)
            RPCProvider(
                name="helius-mainnet",
                url="https://rpc.helius.xyz/?api-key=YOUR_HELIUS_KEY",
                provider_type="helius",
                region="us-east",
                priority=1
            ),
            RPCProvider(
                name="triton-one",
                url="https://api.mainnet-beta.solana.com",
                provider_type="triton",
                region="us-west",
                priority=2
            ),
            
            # Geographic diversity
            RPCProvider(
                name="ankr-germany",
                url="https://rpc.ankr.com/solana",
                provider_type="public",
                region="eu",
                priority=3
            ),
            
            # Self-hosted option (if available)
            RPCProvider(
                name="self-hosted-node",
                url="http://localhost:8899",
                provider_type="self_hosted",
                region="local",
                priority=1
            ),
        ]
    
    async def get_slot_quorum(self) -> QuorumResult:
        """Get current slot with quorum consensus."""
        return await self._execute_quorum_request("getSlot", [])
    
    async def get_transaction_quorum(self, signature: str) -> QuorumResult:
        """Get transaction with quorum consensus."""
        params = [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
        return await self._execute_quorum_request("getTransaction", params)
    
    async def get_recent_prioritization_fees_quorum(self) -> QuorumResult:
        """Get recent prioritization fees with quorum consensus."""
        return await self._execute_quorum_request("getRecentPrioritizationFees", [])
    
    async def get_signatures_for_address_quorum(
        self,
        address: str,
        limit: int = 100,
        before: Optional[str] = None,
        until: Optional[str] = None
    ) -> QuorumResult:
        """Get signatures for address with quorum consensus."""
        params = [address, {"limit": limit}]
        if before:
            params[1]["before"] = before
        if until:
            params[1]["until"] = until
        
        return await self._execute_quorum_request("getSignaturesForAddress", params)
    
    async def _execute_quorum_request(self, method: str, params: List[Any]) -> QuorumResult:
        """Execute RPC request across diverse providers with quorum consensus."""
        provider_responses = {}
        provider_fingerprints = []
        
        # Try providers in priority order
        sorted_providers = sorted(self.providers, key=lambda p: p.priority)
        
        for provider in sorted_providers:
            try:
                response = await self._call_provider(provider, method, params)
                provider_responses[provider.name] = {
                    "success": True,
                    "data": response,
                    "fingerprint": provider.fingerprint(),
                    "provider_type": provider.provider_type,
                    "region": provider.region
                }
                provider_fingerprints.append(provider.fingerprint())
                
            except Exception as e:
                provider_responses[provider.name] = {
                    "success": False,
                    "error": str(e),
                    "fingerprint": provider.fingerprint(),
                    "provider_type": provider.provider_type,
                    "region": provider.region
                }
                provider_fingerprints.append(provider.fingerprint())
        
        # Calculate consensus
        return self._calculate_consensus(provider_responses, provider_fingerprints, method)
    
    async def _call_provider(self, provider: RPCProvider, method: str, params: List[Any]) -> Any:
        """Call individual RPC provider."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        
        # Handle API key substitution
        url = provider.url
        if "YOUR_HELIUS_KEY" in url:
            import os
            helius_key = os.getenv("HELIUS_API_KEY")
            if helius_key:
                url = url.replace("YOUR_HELIUS_KEY", helius_key)
            else:
                raise Exception("Helius API key not configured")
        
        response = await self.client.post(url, json=payload)
        response.raise_for_status()
        
        data = response.json()
        
        if "error" in data:
            raise Exception(f"RPC error: {data['error']}")
        
        return data.get("result")
    
    def _calculate_consensus(
        self,
        provider_responses: Dict[str, Any],
        provider_fingerprints: List[str],
        method: str
    ) -> QuorumResult:
        """Calculate consensus from provider responses."""
        successful_responses = {
            name: resp for name, resp in provider_responses.items()
            if resp.get("success", False)
        }
        
        if len(successful_responses) < self.min_providers:
            return QuorumResult(
                consensus_value=None,
                provider_responses=provider_responses,
                median_value=0.0,
                deviation=100.0,
                consensus_strength=0.0,
                outlier_count=len(provider_responses),
                provider_fingerprints=provider_fingerprints,
                quorum_met=False
            )
        
        # Extract numeric values for consensus calculation
        numeric_values = []
        for name, resp in successful_responses.items():
            value = self._extract_numeric_value(resp["data"], method)
            if value is not None:
                numeric_values.append(value)
        
        if not numeric_values:
            return QuorumResult(
                consensus_value=None,
                provider_responses=provider_responses,
                median_value=0.0,
                deviation=100.0,
                consensus_strength=0.0,
                outlier_count=len(provider_responses),
                provider_fingerprints=provider_fingerprints,
                quorum_met=False
            )
        
        # Calculate median and deviation
        median_value = statistics.median(numeric_values)
        deviations = [abs(v - median_value) for v in numeric_values]
        avg_deviation = statistics.mean(deviations)
        
        # Calculate consensus strength
        consensus_strength = self._calculate_consensus_strength(numeric_values, median_value)
        
        # Identify outliers
        outlier_threshold = median_value * (self.max_deviation_percent / 100)
        outliers = [v for v in numeric_values if abs(v - median_value) > outlier_threshold]
        
        # Determine consensus value (median of non-outliers)
        non_outliers = [v for v in numeric_values if abs(v - median_value) <= outlier_threshold]
        consensus_value = statistics.median(non_outliers) if non_outliers else median_value
        
        # Check if quorum met
        agreement_rate = len(non_outliers) / len(numeric_values)
        quorum_met = agreement_rate >= self.consensus_threshold
        
        return QuorumResult(
            consensus_value=consensus_value,
            provider_responses=provider_responses,
            median_value=median_value,
            deviation=avg_deviation,
            consensus_strength=consensus_strength,
            outlier_count=len(outliers),
            provider_fingerprints=provider_fingerprints,
            quorum_met=quorum_met
        )
    
    def _extract_numeric_value(self, data: Any, method: str) -> Optional[float]:
        """Extract numeric value from RPC response for consensus calculation."""
        try:
            if method == "getSlot":
                return float(data) if data else None
            elif method == "getTransaction":
                # Extract fee or slot from transaction
                if isinstance(data, dict):
                    slot = data.get("slot")
                    fee = data.get("meta", {}).get("fee", 0)
                    return float(fee) if fee else float(slot) if slot else None
            elif method == "getRecentPrioritizationFees":
                # Return average of prioritization fees
                if isinstance(data, list) and data:
                    fees = [f.get("prioritizationFee", 0) for f in data]
                    return statistics.mean(fees) if fees else None
            elif method == "getSignaturesForAddress":
                # Return count of signatures
                if isinstance(data, list):
                    return float(len(data))
            
            return None
        except (ValueError, TypeError, KeyError):
            return None
    
    def _calculate_consensus_strength(self, values: List[float], median: float) -> float:
        """Calculate consensus strength based on value distribution."""
        if not values:
            return 0.0
        
        # Calculate percentage of values close to median
        close_values = [v for v in values if abs(v - median) <= (median * 0.1)]  # Within 10%
        return len(close_values) / len(values)
    
    async def get_provider_health(self) -> Dict[str, Any]:
        """Get health status of all providers."""
        health_results = {}
        
        for provider in self.providers:
            try:
                start_time = datetime.utcnow()
                await self._call_provider(provider, "getSlot", [])
                response_time = (datetime.utcnow() - start_time).total_seconds()
                
                health_results[provider.name] = {
                    "healthy": True,
                    "response_time_ms": response_time * 1000,
                    "fingerprint": provider.fingerprint(),
                    "provider_type": provider.provider_type,
                    "region": provider.region,
                    "priority": provider.priority
                }
                
            except Exception as e:
                health_results[provider.name] = {
                    "healthy": False,
                    "error": str(e),
                    "fingerprint": provider.fingerprint(),
                    "provider_type": provider.provider_type,
                    "region": provider.region,
                    "priority": provider.priority
                }
        
        return {
            "providers": health_results,
            "total_providers": len(self.providers),
            "healthy_providers": len([r for r in health_results.values() if r.get("healthy", False)]),
            "provider_diversity": {
                "types": list(set(p.provider_type for p in self.providers)),
                "regions": list(set(p.region for p in self.providers)),
                "fingerprints": [p.fingerprint() for p in self.providers]
            }
        }
    
    def get_consensus_summary(self) -> Dict[str, Any]:
        """Get quorum engine configuration summary."""
        return {
            "quorum_engine": "Gas Memory Quorum Engine v1.0",
            "consensus_strategy": "median_filtering + deviation_detection",
            "requirements": {
                "min_providers": self.min_providers,
                "max_deviation_percent": self.max_deviation_percent,
                "consensus_threshold": self.consensus_threshold
            },
            "provider_diversity": {
                "total_providers": len(self.providers),
                "provider_types": list(set(p.provider_type for p in self.providers)),
                "regions": list(set(p.region for p in self.providers)),
                "fingerprints": [p.fingerprint() for p in self.providers]
            },
            "security_features": [
                "provider fingerprinting",
                "geographic distribution",
                "backend diversity",
                "outlier detection",
                "consensus strength scoring"
            ]
        }
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()

# Factory function
def get_quorum_engine() -> QuorumEngine:
    """Get quorum engine instance."""
    return QuorumEngine()
