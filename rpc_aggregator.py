"""
Multi-RPC Aggregator with Consensus

Addresses the critique: "1 RPC → 'truth' that's adorable"

Produces consensus-based fee artifacts by:
- Querying 3-5 RPC providers
- Comparing results
- Dropping outliers
- Hashing the consensus
"""
import asyncio
import hashlib
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import httpx
from datetime import datetime


@dataclass
class RPCResult:
    """Result from a single RPC provider."""
    provider: str
    slot: int
    fee_data: Dict[str, int]  # p50, p75, p90, p95, p99
    latency_ms: float
    error: Optional[str] = None


@dataclass
class ConsensusResult:
    """Consensus result from multiple RPC providers."""
    consensus_hash: str
    fees: Dict[str, int]
    providers_used: List[str]
    providers_rejected: List[str]
    outlier_count: int
    timestamp: str
    quorum_reached: bool
    raw_provider_data: List[Dict]  # Raw responses for verification
    strategy_version: str = "1.0.0"  # Version of consensus algorithm


class MultiRPCAggregator:
    """
    Aggregates fee data from multiple RPC providers with outlier rejection.

    Usage:
        aggregator = MultiRPCAggregator()
        result = await aggregator.get_consensus_fees(min_providers=3)
        print(result.consensus_hash)  # Deterministic hash of consensus data
    """

    # Default RPC providers (user can override)
    DEFAULT_PROVIDERS = [
        {"name": "helius", "url": "https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}"},
        {"name": "alchemy", "url": "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"},
        {"name": "quicknode", "url": "https://solana-mainnet.g.alchemy.com/v2/${QUICKNODE_KEY}"},
        {"name": "public", "url": "https://api.mainnet-beta.solana.com"},
        {"name": "ankr", "url": "https://rpc.ankr.com/solana"},
    ]

    def __init__(self, providers: Optional[List[Dict]] = None):
        self.providers = providers or self.DEFAULT_PROVIDERS
        self.client = httpx.AsyncClient(timeout=10.0)

    async def get_consensus_fees(
        self,
        locked_accounts: Optional[List[str]] = None,
        min_providers: int = 3,
        outlier_threshold: float = 2.0  # Standard deviations
    ) -> ConsensusResult:
        """
        Get consensus fee data from multiple RPC providers.

        Args:
            locked_accounts: Accounts to lock for fee calculation
            min_providers: Minimum providers needed for consensus
            outlier_threshold: Z-score threshold for outlier rejection

        Returns:
            ConsensusResult with deterministic hash and metadata
        """
        # Query all providers in parallel
        tasks = [self._query_provider(p, locked_accounts) for p in self.providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter successful results
        successful = []
        failed = []
        for i, result in enumerate(results):
            provider_name = self.providers[i]["name"]
            if isinstance(result, Exception):
                failed.append({"provider": provider_name, "error": str(result)})
            elif result.error:
                failed.append({"provider": provider_name, "error": result.error})
            else:
                successful.append(result)

        # Check quorum
        if len(successful) < min_providers:
            raise ConsensusError(
                f"Quorum failed: only {len(successful)}/{min_providers} providers responded"
            )

        # Calculate consensus for each percentile
        consensus_fees = {}
        rejected = []

        for percentile in ["p50", "p75", "p90", "p95", "p99"]:
            values = [(r.provider, r.fee_data[percentile]) for r in successful]
            consensus_value, outliers = self._calculate_consensus(
                values, outlier_threshold
            )
            consensus_fees[percentile] = consensus_value
            rejected.extend(outliers)

        # Deduplicate rejected providers
        rejected = list(set(rejected))
        used = [r.provider for r in successful if r.provider not in rejected]

        # Generate deterministic consensus hash
        # Hash includes: fees, providers used, timestamp, inputs
        consensus_data = {
            "fees": consensus_fees,
            "providers": sorted(used),
            "timestamp": datetime.utcnow().isoformat(),
            "locked_accounts": locked_accounts or [],
            "schema_version": "1.0.0"
        }

        consensus_json = str(sorted(consensus_data.items()))
        consensus_hash = hashlib.sha256(consensus_json.encode()).hexdigest()[:32]

        # Build raw provider data for complete verification
        raw_provider_data = []
        for result in successful:
            raw_provider_data.append({
                "provider": result.provider,
                "slot": result.slot,
                "latency_ms": round(result.latency_ms, 2),
                "fee_data": result.fee_data,
                "included": result.provider not in rejected,
                "error": result.error
            })

        return ConsensusResult(
            consensus_hash=consensus_hash,
            fees=consensus_fees,
            providers_used=used,
            providers_rejected=rejected,
            outlier_count=len(rejected),
            timestamp=consensus_data["timestamp"],
            quorum_reached=True,
            raw_provider_data=raw_provider_data,
            strategy_version="1.0.0"
        )

    async def _query_provider(
        self,
        provider: Dict,
        locked_accounts: Optional[List[str]]
    ) -> RPCResult:
        """Query a single RPC provider."""
        import time
        start = time.time()

        try:
            url = provider["url"]
            # Replace env vars if present
            if "${" in url:
                import os
                for key in os.environ:
                    url = url.replace(f"${{{key}}}", os.getenv(key, ""))

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getRecentPrioritizationFees",
                "params": [locked_accounts or []]
            }

            response = await self.client.post(url, json=payload)
            data = response.json()
            latency = (time.time() - start) * 1000

            if "error" in data:
                return RPCResult(
                    provider=provider["name"],
                    slot=0,
                    fee_data={},
                    latency_ms=latency,
                    error=data["error"].get("message", "RPC error")
                )

            # Parse fee percentiles
            fees = data.get("result", [])
            if not fees:
                return RPCResult(
                    provider=provider["name"],
                    slot=0,
                    fee_data={"p50": 0, "p75": 0, "p90": 0, "p95": 0, "p99": 0},
                    latency_ms=latency,
                    error="No fee data"
                )

            # Calculate percentiles
            fee_values = sorted([f["prioritizationFee"] for f in fees])

            def percentile(p: float) -> int:
                idx = int(len(fee_values) * p / 100)
                return fee_values[max(0, min(idx, len(fee_values) - 1))]

            fee_data = {
                "p50": percentile(50),
                "p75": percentile(75),
                "p90": percentile(90),
                "p95": percentile(95),
                "p99": percentile(99)
            }

            return RPCResult(
                provider=provider["name"],
                slot=data.get("result", [{}])[0].get("slot", 0),
                fee_data=fee_data,
                latency_ms=latency
            )

        except Exception as e:
            return RPCResult(
                provider=provider["name"],
                slot=0,
                fee_data={},
                latency_ms=0,
                error=str(e)
            )

    def _calculate_consensus(
        self,
        values: List[Tuple[str, int]],
        threshold: float
    ) -> Tuple[int, List[str]]:
        """
        Calculate consensus value with outlier rejection.

        Uses median absolute deviation (MAD) for robust outlier detection.
        """
        if not values:
            return 0, []

        if len(values) == 1:
            return values[0][1], []

        # Calculate median
        sorted_values = sorted(values, key=lambda x: x[1])
        median = sorted_values[len(sorted_values) // 2][1]

        # Calculate MAD (median absolute deviation)
        deviations = [abs(v[1] - median) for v in values]
        mad = sorted(deviations)[len(deviations) // 2] if deviations else 1
        if mad == 0:
            mad = 1  # Avoid division by zero

        # Z-score using MAD (more robust than standard deviation)
        outliers = []
        for provider, value in values:
            z_score = abs(value - median) / mad
            if z_score > threshold:
                outliers.append(provider)

        # Consensus is median of non-outliers
        valid_values = [v for p, v in values if p not in outliers]
        if not valid_values:
            valid_values = [v for p, v in values]  # Fallback to all

        consensus = sorted(valid_values)[len(valid_values) // 2]

        return consensus, outliers

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


class ConsensusError(Exception):
    """Raised when consensus cannot be reached."""
    pass


# Global instance
_aggregator: Optional[MultiRPCAggregator] = None


async def get_aggregator() -> MultiRPCAggregator:
    """Get or create aggregator instance."""
    global _aggregator
    if _aggregator is None:
        _aggregator = MultiRPCAggregator()
    return _aggregator
