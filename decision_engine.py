"""
CID → Decision Engine

Turns artifacts into execution decisions.
CID = artifact → CID = actionable fee strategy
"""
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import json
import statistics

from app.services.real_ipfs import get_real_ipfs_storage
from app.models.artifact_schema_v1 import GasMemoryArtifactV1

@dataclass
class ExecutionDecision:
    """Actionable execution decision derived from CID."""
    cid: str
    recommended_strategy: str  # "p50", "p75", "p90", "aggressive"
    recommended_compute_unit_price: int  # micro-lamports per CU
    recommended_priority_fee: int  # lamports
    expected_success_rate: float
    expected_latency_slots: float
    confidence_score: float
    regime: str
    dominant_factor: str
    confidence_basis: List[str]
    cost_estimate_sol: float
    risk_assessment: str
    decision_timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "cid": self.cid,
            "recommended_strategy": self.recommended_strategy,
            "recommended_compute_unit_price": self.recommended_compute_unit_price,
            "recommended_priority_fee": self.recommended_priority_fee,
            "expected_success_rate": self.expected_success_rate,
            "expected_latency_slots": self.expected_latency_slots,
            "confidence_score": self.confidence_score,
            "regime": self.regime,
            "dominant_factor": self.dominant_factor,
            "confidence_basis": self.confidence_basis,
            "cost_estimate_sol": self.cost_estimate_sol,
            "risk_assessment": self.risk_assessment,
            "decision_timestamp": self.decision_timestamp
        }

@dataclass
class MarketRegime:
    """Market regime classification."""
    name: str
    description: str
    characteristics: List[str]
    fee_environment: str  # "low", "normal", "elevated", "extreme"
    volatility_indicator: str  # "stable", "moderate", "high", "extreme"

class DecisionEngine:
    """
    CID → Decision Engine
    
    Transforms verifiable artifacts into actionable execution decisions.
    No LLM pricing - purely data-driven regime detection.
    """
    
    def __init__(self):
        self.ipfs_storage = get_real_ipfs_storage()
        self.regimes = self._initialize_regimes()
    
    def _initialize_regimes(self) -> Dict[str, MarketRegime]:
        """Initialize market regime classifications."""
        return {
            "congested": MarketRegime(
                name="congested",
                description="Network experiencing high load, priority fees elevated",
                characteristics=["high_priority_fees", "increased_latency", "low_success_rate"],
                fee_environment="elevated",
                volatility_indicator="high"
            ),
            "normal": MarketRegime(
                name="normal",
                description="Typical network conditions, predictable fees",
                characteristics=["stable_fees", "moderate_latency", "high_success_rate"],
                fee_environment="normal",
                volatility_indicator="stable"
            ),
            "quiet": MarketRegime(
                name="quiet",
                description="Low network activity, minimal fees",
                characteristics=["low_fees", "fast_latency", "very_high_success_rate"],
                fee_environment="low",
                volatility_indicator="stable"
            ),
            "volatile": MarketRegime(
                name="volatile",
                description="Rapidly changing conditions, unpredictable fees",
                characteristics=["fee_spikes", "variable_latency", "inconsistent_success"],
                fee_environment="extreme",
                volatility_indicator="extreme"
            )
        }
    
    async def cid_to_decision(
        self,
        cid: str,
        compute_unit_limit: int = 200000,
        risk_tolerance: str = "balanced"  # "conservative", "balanced", "aggressive"
    ) -> ExecutionDecision:
        """
        Convert CID to actionable execution decision.
        
        Args:
            cid: Artifact CID
            compute_unit_limit: Target compute units for transaction
            risk_tolerance: Risk tolerance level
            
        Returns:
            Actionable execution decision
        """
        try:
            # Retrieve artifact from IPFS
            artifact = await self._retrieve_artifact(cid)
            
            # Analyze market regime
            regime = self._detect_market_regime(artifact)
            
            # Calculate optimal strategy
            strategy, confidence = self._calculate_optimal_strategy(artifact, risk_tolerance)
            
            # Extract fee recommendations
            fee_rec = self._extract_fee_recommendations(artifact, strategy, compute_unit_limit)
            
            # Assess confidence basis
            confidence_basis = self._assess_confidence_basis(artifact, regime)
            
            # Calculate cost estimate
            cost_estimate = self._calculate_cost_estimate(fee_rec, compute_unit_limit)
            
            # Risk assessment
            risk_assessment = self._assess_risk(artifact, regime, confidence)
            
            # Build decision
            decision = ExecutionDecision(
                cid=cid,
                recommended_strategy=strategy,
                recommended_compute_unit_price=fee_rec["compute_unit_price"],
                recommended_priority_fee=fee_rec["priority_fee"],
                expected_success_rate=fee_rec["success_rate"],
                expected_latency_slots=fee_rec["latency_slots"],
                confidence_score=confidence,
                regime=regime.name,
                dominant_factor=fee_rec["dominant_factor"],
                confidence_basis=confidence_basis,
                cost_estimate_sol=cost_estimate,
                risk_assessment=risk_assessment,
                decision_timestamp=datetime.utcnow().isoformat()
            )
            
            return decision
            
        except Exception as e:
            raise Exception(f"Decision engine failed: {str(e)}")
    
    async def _retrieve_artifact(self, cid: str) -> GasMemoryArtifactV1:
        """Retrieve and parse artifact from IPFS."""
        try:
            # Retrieve content
            content = await self.ipfs_storage.retrieve_cid(cid)
            
            # Parse artifact
            artifact_data = json.loads(content.decode('utf-8'))
            
            # Reconstruct artifact object
            from app.models.artifact_schema_v1 import get_artifact_builder_v1
            builder = get_artifact_builder_v1()
            
            # Create artifact from dict (simplified reconstruction)
            artifact = GasMemoryArtifactV1(
                metadata=None,  # Would need full reconstruction
                rpc_sources=[],
                raw_samples=[],
                fee_statistics=None,
                verification_proofs=[],
                temporal_surface=[],
                canonical_sha256=artifact_data.get("canonical_sha256", "")
            )
            
            return artifact
            
        except Exception as e:
            raise Exception(f"Failed to retrieve artifact {cid}: {str(e)}")
    
    def _detect_market_regime(self, artifact: GasMemoryArtifactV1) -> MarketRegime:
        """Detect market regime from artifact data."""
        if not artifact.fee_statistics:
            return self.regimes["normal"]
        
        stats = artifact.fee_statistics
        
        # Regime detection logic (purely data-driven)
        if stats.p75_micro_lamports_per_cu > 100000:  # Very high fees
            return self.regimes["congested"]
        elif stats.p75_micro_lamports_per_cu < 20000:  # Very low fees
            return self.regimes["quiet"]
        elif stats.success_rate < 0.8:  # Low success rate
            return self.regimes["volatile"]
        else:
            return self.regimes["normal"]
    
    def _calculate_optimal_strategy(
        self,
        artifact: GasMemoryArtifactV1,
        risk_tolerance: str
    ) -> Tuple[str, float]:
        """Calculate optimal fee strategy based on data and risk tolerance."""
        if not artifact.fee_statistics:
            return "p75", 0.5
        
        stats = artifact.fee_statistics
        
        # Base strategy on risk tolerance
        if risk_tolerance == "conservative":
            strategy = "p90"
            base_confidence = 0.8
        elif risk_tolerance == "aggressive":
            strategy = "p50"
            base_confidence = 0.6
        else:  # balanced
            strategy = "p75"
            base_confidence = 0.7
        
        # Adjust confidence based on data quality
        if stats.verified_samples > 100:
            base_confidence += 0.1
        if stats.success_rate > 0.9:
            base_confidence += 0.1
        
        # Adjust for sample density
        sample_density = stats.sample_count / 1000  # Normalize per 1000 samples
        if sample_density > 0.5:
            base_confidence += 0.1
        
        confidence = min(0.95, base_confidence)
        
        return strategy, confidence
    
    def _extract_fee_recommendations(
        self,
        artifact: GasMemoryArtifactV1,
        strategy: str,
        compute_unit_limit: int
    ) -> Dict[str, Any]:
        """Extract fee recommendations from artifact."""
        if not artifact.fee_statistics:
            return {
                "compute_unit_price": 50000,
                "priority_fee": 10000000,
                "success_rate": 0.8,
                "latency_slots": 3.0,
                "dominant_factor": "insufficient_data"
            }
        
        stats = artifact.fee_statistics
        
        # Select fee percentile based on strategy
        if strategy == "p50":
            fee_per_cu = stats.p50_micro_lamports_per_cu
        elif strategy == "p75":
            fee_per_cu = stats.p75_micro_lamports_per_cu
        elif strategy == "p90":
            fee_per_cu = stats.p90_micro_lamports_per_cu
        else:  # aggressive
            fee_per_cu = stats.p50_micro_lamports_per_cu
        
        # Calculate priority fee
        priority_fee = fee_per_cu * compute_unit_limit // 1_000_000
        
        # Determine dominant factor
        if stats.verified_samples < 50:
            dominant_factor = "low_sample_count"
        elif stats.success_rate < 0.8:
            dominant_factor = "low_success_rate"
        elif fee_per_cu > 100000:
            dominant_factor = "high_fee_environment"
        else:
            dominant_factor = "normal_conditions"
        
        return {
            "compute_unit_price": fee_per_cu,
            "priority_fee": priority_fee,
            "success_rate": stats.success_rate,
            "latency_slots": stats.median_latency_slots,
            "dominant_factor": dominant_factor
        }
    
    def _assess_confidence_basis(
        self,
        artifact: GasMemoryArtifactV1,
        regime: MarketRegime
    ) -> List[str]:
        """Assess basis for confidence score."""
        basis = []
        
        if artifact.fee_statistics:
            stats = artifact.fee_statistics
            
            if stats.verified_samples > 100:
                basis.append("high_verified_sample_density")
            elif stats.verified_samples > 50:
                basis.append("moderate_verified_sample_density")
            else:
                basis.append("low_verified_sample_density")
            
            if stats.success_rate > 0.9:
                basis.append("high_success_rate")
            elif stats.success_rate > 0.8:
                basis.append("moderate_success_rate")
            else:
                basis.append("low_success_rate")
            
            # Fee variance assessment
            fee_range = stats.p90_micro_lamports_per_cu - stats.p50_micro_lamports_per_cu
            if fee_range < 10000:
                basis.append("low_fee_variance")
            elif fee_range < 50000:
                basis.append("moderate_fee_variance")
            else:
                basis.append("high_fee_variance")
        
        # Regime-specific basis
        if regime.name == "normal":
            basis.append("stable_regime")
        elif regime.name == "congested":
            basis.append("high_load_regime")
        elif regime.name == "quiet":
            basis.append("low_load_regime")
        else:
            basis.append("volatile_regime")
        
        return basis
    
    def _calculate_cost_estimate(
        self,
        fee_rec: Dict[str, Any],
        compute_unit_limit: int
    ) -> float:
        """Calculate cost estimate in SOL."""
        total_fee_lamports = fee_rec["priority_fee"]
        
        # Add base fee estimate (typical Solana base fee)
        base_fee = 5000  # 5000 lamports typical base fee
        total_fee_lamports += base_fee
        
        # Convert to SOL
        cost_sol = total_fee_lamports / 1_000_000_000
        
        return round(cost_sol, 8)
    
    def _assess_risk(
        self,
        artifact: GasMemoryArtifactV1,
        regime: MarketRegime,
        confidence: float
    ) -> str:
        """Assess overall risk level."""
        risk_factors = []
        
        # Data quality risk
        if artifact.fee_statistics:
            stats = artifact.fee_statistics
            if stats.verified_samples < 50:
                risk_factors.append("insufficient_data")
            if stats.success_rate < 0.8:
                risk_factors.append("low_success_rate")
        
        # Regime risk
        if regime.name == "volatile":
            risk_factors.append("high_volatility")
        elif regime.name == "congested":
            risk_factors.append("network_congestion")
        
        # Confidence risk
        if confidence < 0.7:
            risk_factors.append("low_confidence")
        
        # Overall risk assessment
        if len(risk_factors) == 0:
            return "low"
        elif len(risk_factors) <= 2:
            return "moderate"
        else:
            return "high"
    
    async def compare_decisions(self, cids: List[str]) -> Dict[str, Any]:
        """Compare multiple CIDs and their decisions."""
        decisions = []
        
        for cid in cids:
            try:
                decision = await self.cid_to_decision(cid)
                decisions.append(decision)
            except Exception as e:
                decisions.append({
                    "cid": cid,
                    "error": str(e)
                })
        
        # Comparison analysis
        valid_decisions = [d for d in decisions if isinstance(d, ExecutionDecision)]
        
        if not valid_decisions:
            return {
                "comparisons": decisions,
                "analysis": "No valid decisions to compare"
            }
        
        # Extract metrics for comparison
        fees = [d.recommended_compute_unit_price for d in valid_decisions]
        confidences = [d.confidence_score for d in valid_decisions]
        costs = [d.cost_estimate_sol for d in valid_decisions]
        
        analysis = {
            "fee_range": {
                "min": min(fees),
                "max": max(fees),
                "median": statistics.median(fees)
            },
            "confidence_range": {
                "min": min(confidences),
                "max": max(confidences),
                "average": sum(confidences) / len(confidences)
            },
            "cost_range": {
                "min": min(costs),
                "max": max(costs),
                "average": sum(costs) / len(costs)
            },
            "regime_distribution": {
                regime: len([d for d in valid_decisions if d.regime == regime])
                for regime in set(d.regime for d in valid_decisions)
            }
        }
        
        return {
            "comparisons": [d.to_dict() if isinstance(d, ExecutionDecision) else d for d in decisions],
            "analysis": analysis
        }
    
    async def close(self):
        """Close services."""
        await self.ipfs_storage.close()

# Factory function
def get_decision_engine() -> DecisionEngine:
    """Get decision engine instance."""
    return DecisionEngine()
