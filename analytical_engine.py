"""
Analytical Engine for Gas Fee Analysis
Replaces decorative LLM with deterministic, analytical scoring.
"""
import numpy as np
from typing import List, Dict, Any
from dataclasses import dataclass
import statistics

from app.models.schemas import FeeSample, FeeCurve, LLMPolicy, LLMInterpretation

@dataclass
class ExecutionScore:
    """Execution score with components."""
    overall_score: float
    success_rate_weight: float
    latency_weight: float
    efficiency_weight: float
    confidence: float
    risk_level: str

class AnalyticalEngine:
    """
    Deterministic analytical engine for gas fee analysis.
    
    Replaces LLM with mathematical models that provide:
    - Deterministic outputs
    - Explainable reasoning
    - Comparable scores
    - Structured recommendations
    """
    
    def __init__(self):
        self.scoring_weights = {
            'success_rate': 0.4,
            'latency_inverse': 0.3,
            'fee_efficiency': 0.3
        }
        
        # Risk thresholds based on success rate
        self.risk_thresholds = {
            'low': 0.95,
            'medium': 0.85,
            'high': 0.70
        }
    
    def calculate_execution_score(self, samples: List[FeeSample]) -> ExecutionScore:
        """
        Calculate execution score from fee samples.
        
        Args:
            samples: List of fee samples
            
        Returns:
            ExecutionScore with detailed breakdown
        """
        if not samples:
            return ExecutionScore(0.0, 0.0, 0.0, 0.0, 0.0, "unknown")
        
        # Extract metrics
        success_rates = [1.0 if s.success else 0.0 for s in samples]
        latencies = [s.confirmation_latency_slots for s in samples if s.success]
        fee_efficiencies = []
        
        for s in samples:
            if s.success and s.compute_units_consumed > 0:
                # Efficiency = compute units per lamport (higher is better)
                efficiency = s.compute_units_consumed / max(s.total_fee_lamports, 1)
                fee_efficiencies.append(efficiency)
        
        # Calculate components
        success_rate = statistics.mean(success_rates)
        
        # Latency score (inverse - lower latency = higher score)
        if latencies:
            avg_latency = statistics.mean(latencies)
            latency_score = 1.0 / (1.0 + avg_latency)  # Normalize to 0-1
        else:
            latency_score = 0.0
        
        # Fee efficiency score (normalized)
        if fee_efficiencies:
            avg_efficiency = statistics.mean(fee_efficiencies)
            # Normalize efficiency (assuming reasonable range)
            efficiency_score = min(avg_efficiency / 1000.0, 1.0)
        else:
            efficiency_score = 0.0
        
        # Calculate weighted score
        overall_score = (
            success_rate * self.scoring_weights['success_rate'] +
            latency_score * self.scoring_weights['latency_inverse'] +
            efficiency_score * self.scoring_weights['fee_efficiency']
        )
        
        # Confidence based on sample size and variance
        sample_size = len(samples)
        variance_penalty = min(statistics.variance(success_rates) if len(success_rates) > 1 else 0, 0.25)
        confidence = min(sample_size / 1000.0, 1.0) * (1.0 - variance_penalty)
        
        # Risk assessment
        if success_rate >= self.risk_thresholds['low']:
            risk_level = "low"
        elif success_rate >= self.risk_thresholds['medium']:
            risk_level = "medium"
        elif success_rate >= self.risk_thresholds['high']:
            risk_level = "high"
        else:
            risk_level = "very_high"
        
        return ExecutionScore(
            overall_score=overall_score,
            success_rate_weight=success_rate,
            latency_weight=latency_score,
            efficiency_weight=efficiency_score,
            confidence=confidence,
            risk_level=risk_level
        )
    
    def generate_fee_curves(self, samples: List[FeeSample]) -> FeeCurve:
        """
        Generate fee curves from samples.
        
        Args:
            samples: List of fee samples
            
        Returns:
            FeeCurve with percentile values
        """
        if not samples:
            return FeeCurve(p50=0, p75=0, p90=0)
        
        # Calculate micro-lamports per compute unit for successful transactions
        fees_per_cu = []
        for s in samples:
            if s.success and s.compute_units_consumed > 0:
                fee_per_cu = s.total_fee_lamports / s.compute_units_consumed
                fees_per_cu.append(fee_per_cu)
        
        if not fees_per_cu:
            return FeeCurve(p50=0, p75=0, p90=0)
        
        # Calculate percentiles
        p50 = np.percentile(fees_per_cu, 50)
        p75 = np.percentile(fees_per_cu, 75)
        p90 = np.percentile(fees_per_cu, 90)
        
        return FeeCurve(
            p50_micro_lamports_per_cu=int(p50),
            p75_micro_lamports_per_cu=int(p75),
            p90_micro_lamports_per_cu=int(p90)
        )
    
    def create_interpretation(self, samples: List[FeeSample], score: ExecutionScore) -> LLMInterpretation:
        """
        Create analytical interpretation.
        
        Args:
            samples: List of fee samples
            score: Execution score
            
        Returns:
            LLMInterpretation with analytical insights
        """
        # Generate deterministic policy recommendations
        policy = self._generate_policy(score)
        
        # Generate reasoning
        reason = self._generate_reasoning(samples, score)
        
        # Detect anomalies
        anomalies = self._detect_anomalies(samples)
        
        return LLMInterpretation(
            model="analytical_engine_v1.0",
            recommended_policy=policy,
            reason=reason,
            confidence_score=score.confidence,
            risk_assessment=f"{score.risk_level}_risk",
            anomalies_detected=anomalies if anomalies else None
        )
    
    def _generate_policy(self, score: ExecutionScore) -> LLMPolicy:
        """Generate policy recommendations based on score."""
        base_fee = 1000  # Base micro-lamports per CU
        
        # Adjust based on risk level
        risk_multipliers = {
            "low": {"cheap": 0.8, "balanced": 1.0, "urgent": 1.2},
            "medium": {"cheap": 1.0, "balanced": 1.3, "urgent": 1.6},
            "high": {"cheap": 1.3, "balanced": 1.8, "urgent": 2.5},
            "very_high": {"cheap": 1.8, "balanced": 2.5, "urgent": 3.5}
        }
        
        multipliers = risk_multipliers.get(score.risk_level, risk_multipliers["medium"])
        
        return LLMPolicy(
            cheap=f"p{int(base_fee * multipliers['cheap'])}",
            balanced=f"p{int(base_fee * multipliers['balanced'])}",
            urgent=f"p{int(base_fee * multipliers['urgent'])}"
        )
    
    def _generate_reasoning(self, samples: List[FeeSample], score: ExecutionScore) -> str:
        """Generate analytical reasoning."""
        if not samples:
            return "Insufficient data for analysis."
        
        sample_count = len(samples)
        success_rate = score.success_rate_weight
        avg_latency = statistics.mean([s.confirmation_latency_slots for s in samples if s.success]) if any(s.success for s in samples) else 0
        
        reasoning_parts = []
        
        # Success rate analysis
        if success_rate >= 0.95:
            reasoning_parts.append(f"Excellent success rate ({success_rate:.1%}) indicates reliable execution")
        elif success_rate >= 0.85:
            reasoning_parts.append(f"Good success rate ({success_rate:.1%}) with moderate reliability")
        elif success_rate >= 0.70:
            reasoning_parts.append(f"Moderate success rate ({success_rate:.1%}) suggests higher volatility")
        else:
            reasoning_parts.append(f"Low success rate ({success_rate:.1%}) indicates high risk")
        
        # Latency analysis
        if avg_latency <= 1:
            reasoning_parts.append(f"Fast confirmation ({avg_latency:.1f} slots)")
        elif avg_latency <= 3:
            reasoning_parts.append(f"Moderate confirmation ({avg_latency:.1f} slots)")
        else:
            reasoning_parts.append(f"Slow confirmation ({avg_latency:.1f} slots)")
        
        # Sample size confidence
        if sample_count >= 1000:
            reasoning_parts.append("High confidence with large sample size")
        elif sample_count >= 100:
            reasoning_parts.append("Moderate confidence with adequate sample size")
        else:
            reasoning_parts.append("Low confidence with small sample size")
        
        return ". ".join(reasoning_parts) + "."
    
    def _detect_anomalies(self, samples: List[FeeSample]) -> List[str]:
        """Detect anomalies in the data."""
        anomalies = []
        
        if len(samples) < 10:
            return anomalies
        
        # Check for unusual failure patterns
        success_rates = [1.0 if s.success else 0.0 for s in samples]
        if len(success_rates) > 1:
            failure_rate = 1.0 - statistics.mean(success_rates)
            if failure_rate > 0.5:
                anomalies.append(f"High failure rate ({failure_rate:.1%})")
        
        # Check for latency outliers
        latencies = [s.confirmation_latency_slots for s in samples if s.success]
        if len(latencies) > 10:
            q75 = np.percentile(latencies, 75)
            outlier_count = sum(1 for latency_val in latencies if latency_val > q75 * 2)
            if outlier_count > len(latencies) * 0.1:
                anomalies.append(f"Many latency outliers ({outlier_count}/{len(latencies)})")
        
        # Check for fee anomalies
        fees = [s.total_fee_lamports for s in samples if s.success]
        if len(fees) > 10:
            q75 = np.percentile(fees, 75)
            high_fees = [f for f in fees if f > q75 * 3]
            if len(high_fees) > len(fees) * 0.05:
                anomalies.append(f"High fee outliers detected")
        
        return anomalies
    
    def simulate_optimal_strategy(self, samples: List[FeeSample], strategy: str) -> Dict[str, Any]:
        """
        Simulate optimal fee strategy.
        
        Args:
            samples: Historical fee samples
            strategy: Strategy type ('cheap', 'balanced', 'urgent')
            
        Returns:
            Simulation results
        """
        if not samples:
            return {"error": "No samples available for simulation"}
        
        # Get fee curve
        fee_curve = self.generate_fee_curves(samples)
        
        # Select target fee based on strategy
        if strategy == "cheap":
            target_fee = fee_curve.p50_micro_lamports_per_cu
        elif strategy == "balanced":
            target_fee = fee_curve.p75_micro_lamports_per_cu
        else:  # urgent
            target_fee = fee_curve.p90_micro_lamports_per_cu
        
        # Simulate outcomes
        outcomes = []
        for sample in samples:
            if not sample.success:
                continue
                
            sample_fee_per_cu = sample.total_fee_lamports / sample.compute_units_consumed
            
            # Determine if strategy would have succeeded
            if strategy == "cheap":
                would_succeed = sample_fee_per_cu <= target_fee * 1.1
            elif strategy == "balanced":
                would_succeed = sample_fee_per_cu <= target_fee * 1.2
            else:  # urgent
                would_succeed = True  # Urgent always succeeds by definition
            
            if would_succeed:
                savings = max(0, sample_fee_per_cu - target_fee) * sample.compute_units_consumed
                outcomes.append({
                    "success": True,
                    "savings": savings,
                    "latency": sample.confirmation_latency_slots
                })
            else:
                outcomes.append({
                    "success": False,
                    "savings": 0,
                    "latency": float('inf')
                })
        
        # Calculate simulation metrics
        if outcomes:
            success_rate = sum(1 for o in outcomes if o["success"]) / len(outcomes)
            avg_savings = statistics.mean([o["savings"] for o in outcomes if o["success"]]) if any(o["success"] for o in outcomes) else 0
            avg_latency = statistics.mean([o["latency"] for o in outcomes if o["success"] and o["latency"] != float('inf')]) if any(o["success"] and o["latency"] != float('inf') for o in outcomes) else 0
        else:
            success_rate = 0.0
            avg_savings = 0.0
            avg_latency = 0.0
        
        return {
            "strategy": strategy,
            "target_fee_per_cu": target_fee,
            "simulated_success_rate": success_rate,
            "average_savings_lamports": avg_savings,
            "average_latency_slots": avg_latency,
            "samples_simulated": len(outcomes)
        }
