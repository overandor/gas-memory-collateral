"""
Quality Enforcer

Enforces verification quality thresholds.
Rejects artifacts that don't meet minimum standards.
"""
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

from app.models.gas_memory_post import GasMemoryPost
from app.models.provenance import FeeSample

@dataclass
class QualityThresholds:
    """Quality thresholds for artifact acceptance."""
    min_verified_samples: int = 50
    min_verification_rate: float = 0.7  # 70%
    min_success_rate: float = 0.5  # 50%
    max_zero_fee_samples: float = 0.3  # Max 30% zero fees
    min_sample_count: int = 100
    max_latency_outliers: float = 0.1  # Max 10% extreme latencies

@dataclass
class QualityReport:
    """Quality assessment report."""
    passed: bool
    thresholds: QualityThresholds
    metrics: Dict[str, Any]
    violations: List[str]
    warnings: List[str]
    recommendations: List[str]
    overall_score: float

class QualityEnforcer:
    """
    Enforces quality standards on Gas Memory artifacts.
    
    Rejects artifacts that don't meet minimum verification and data quality standards.
    """
    
    def __init__(self, thresholds: QualityThresholds = None):
        self.thresholds = thresholds or QualityThresholds()
    
    def assess_artifact_quality(
        self,
        artifact: GasMemoryPost,
        samples: List[FeeSample],
        verification_proofs: List[Dict[str, Any]]
    ) -> QualityReport:
        """Assess artifact quality against thresholds."""
        metrics = self._calculate_metrics(artifact, samples, verification_proofs)
        violations = []
        warnings = []
        recommendations = []
        
        # Check minimum verified samples
        if metrics["verified_samples"] < self.thresholds.min_verified_samples:
            violations.append(
                f"Insufficient verified samples: {metrics['verified_samples']} < {self.thresholds.min_verified_samples}"
            )
            recommendations.append("Increase time window or use block scanning for better coverage")
        
        # Check verification rate
        if metrics["verification_rate"] < self.thresholds.min_verification_rate:
            violations.append(
                f"Low verification rate: {metrics['verification_rate']:.1%} < {self.thresholds.min_verification_rate:.1%}"
            )
            recommendations.append("Check RPC provider health and network connectivity")
        
        # Check success rate
        if metrics["success_rate"] < self.thresholds.min_success_rate:
            violations.append(
                f"Low success rate: {metrics['success_rate']:.1%} < {self.thresholds.min_success_rate:.1%}"
            )
            recommendations.append("Investigate transaction failure patterns and network conditions")
        
        # Check zero fee samples
        if metrics["zero_fee_rate"] > self.thresholds.max_zero_fee_samples:
            violations.append(
                f"Too many zero fee samples: {metrics['zero_fee_rate']:.1%} > {self.thresholds.max_zero_fee_samples:.1%}"
            )
            recommendations.append("Fix fee extraction logic - priority fees not being captured")
        
        # Check minimum sample count
        if metrics["total_samples"] < self.thresholds.min_sample_count:
            violations.append(
                f"Insufficient total samples: {metrics['total_samples']} < {self.thresholds.min_sample_count}"
            )
            recommendations.append("Extend time window or target higher-activity pools")
        
        # Check for extreme latencies
        if metrics["extreme_latency_rate"] > self.thresholds.max_latency_outliers:
            warnings.append(
                f"High extreme latency rate: {metrics['extreme_latency_rate']:.1%} > {self.thresholds.max_latency_outliers:.1%}"
            )
            recommendations.append("Consider filtering extreme latencies or investigating network congestion")
        
        # Check fee distribution quality
        if metrics["fee_range_ratio"] > 1000:  # 1000x ratio indicates poor data
            warnings.append(
                f"Extreme fee range ratio: {metrics['fee_range_ratio']:.1f} - possible data quality issues"
            )
            recommendations.append("Review fee extraction for potential parsing errors")
        
        # Check temporal coverage
        if metrics["time_coverage_hours"] < 1.0:
            warnings.append(
                f"Limited time coverage: {metrics['time_coverage_hours']:.1f} hours"
            )
            recommendations.append("Increase time window for better temporal representation")
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(metrics, violations, warnings)
        
        # Determine if passed
        passed = len(violations) == 0 and overall_score >= 0.7
        
        return QualityReport(
            passed=passed,
            thresholds=self.thresholds,
            metrics=metrics,
            violations=violations,
            warnings=warnings,
            recommendations=recommendations,
            overall_score=overall_score
        )
    
    def _calculate_metrics(
        self,
        artifact: GasMemoryPost,
        samples: List[FeeSample],
        verification_proofs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate quality metrics."""
        if not samples:
            return {
                "total_samples": 0,
                "verified_samples": 0,
                "verification_rate": 0.0,
                "success_rate": 0.0,
                "zero_fee_rate": 1.0,
                "extreme_latency_rate": 0.0,
                "fee_range_ratio": 0.0,
                "time_coverage_hours": 0.0,
                "provider_diversity": 0.0
            }
        
        # Basic counts
        total_samples = len(samples)
        verified_samples = len([s for s in samples if s.verified])
        successful_samples = len([s for s in samples if s.success])
        zero_fee_samples = len([s for s in samples if s.compute_unit_price_micro_lamports == 0])
        
        # Rates
        verification_rate = verified_samples / total_samples if total_samples > 0 else 0.0
        success_rate = successful_samples / total_samples if total_samples > 0 else 0.0
        zero_fee_rate = zero_fee_samples / total_samples if total_samples > 0 else 0.0
        
        # Fee analysis
        fees = [s.compute_unit_price_micro_lamports for s in samples if s.compute_unit_price_micro_lamports > 0]
        if fees:
            min_fee = min(fees)
            max_fee = max(fees)
            fee_range_ratio = max_fee / min_fee if min_fee > 0 else float('inf')
        else:
            fee_range_ratio = float('inf')
        
        # Latency analysis
        latencies = [s.confirmation_latency_slots for s in samples if s.confirmation_latency_slots > 0]
        if latencies:
            median_latency = sorted(latencies)[len(latencies) // 2]
            extreme_threshold = median_latency * 10
            extreme_latencies = len([l for l in latencies if l > extreme_threshold])
            extreme_latency_rate = extreme_latencies / len(latencies)
        else:
            extreme_latency_rate = 0.0
        
        # Time coverage
        if samples:
            earliest_time = min(s.block_time for s in samples)
            latest_time = max(s.block_time for s in samples)
            time_coverage_hours = (latest_time - earliest_time).total_seconds() / 3600
        else:
            time_coverage_hours = 0.0
        
        # Provider diversity
        providers = set(s.source_provider for s in samples if s.source_provider)
        provider_diversity = len(providers) / 5.0  # Normalize to 0-1 (assuming max 5 providers)
        
        return {
            "total_samples": total_samples,
            "verified_samples": verified_samples,
            "verification_rate": verification_rate,
            "success_rate": success_rate,
            "zero_fee_rate": zero_fee_rate,
            "extreme_latency_rate": extreme_latency_rate,
            "fee_range_ratio": fee_range_ratio,
            "time_coverage_hours": time_coverage_hours,
            "provider_diversity": provider_diversity,
            "provider_count": len(providers),
            "unique_program_ids": len(set(pid for s in samples for pid in s.program_ids)),
            "verification_proofs_count": len(verification_proofs)
        }
    
    def _calculate_overall_score(
        self,
        metrics: Dict[str, Any],
        violations: List[str],
        warnings: List[str]
    ) -> float:
        """Calculate overall quality score."""
        base_score = 1.0
        
        # Penalize violations heavily
        violation_penalty = len(violations) * 0.2
        base_score -= violation_penalty
        
        # Penalize warnings lightly
        warning_penalty = len(warnings) * 0.05
        base_score -= warning_penalty
        
        # Bonus for good metrics
        if metrics["verification_rate"] >= 0.9:
            base_score += 0.1
        if metrics["success_rate"] >= 0.8:
            base_score += 0.1
        if metrics["zero_fee_rate"] <= 0.1:
            base_score += 0.1
        if metrics["provider_diversity"] >= 0.6:
            base_score += 0.05
        
        # Clamp to 0-1 range
        return max(0.0, min(1.0, base_score))
    
    def enforce_quality(
        self,
        artifact: GasMemoryPost,
        samples: List[FeeSample],
        verification_proofs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Enforce quality - reject if not meeting standards."""
        quality_report = self.assess_artifact_quality(artifact, samples, verification_proofs)
        
        enforcement_result = {
            "artifact_accepted": quality_report.passed,
            "quality_report": quality_report,
            "enforcement_timestamp": datetime.utcnow().isoformat(),
            "thresholds_used": self.thresholds.__dict__
        }
        
        if not quality_report.passed:
            enforcement_result["rejection_reasons"] = quality_report.violations
            enforcement_result["required_improvements"] = quality_report.recommendations
        
        return enforcement_result
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get quality enforcement summary."""
        return {
            "quality_enforcer": "Gas Memory Quality Enforcer v1.0",
            "thresholds": self.thresholds.__dict__,
            "enforcement_strategy": "reject_on_violations",
            "quality_dimensions": [
                {
                    "dimension": "verification_quality",
                    "threshold": f"{self.thresholds.min_verification_rate:.1%} verification rate",
                    "description": "Ensures samples are cryptographically verified"
                },
                {
                    "dimension": "data_completeness",
                    "threshold": f"{self.thresholds.min_verified_samples} verified samples",
                    "description": "Ensures sufficient ground truth data"
                },
                {
                    "dimension": "fee_extraction",
                    "threshold": f"≤{self.thresholds.max_zero_fee_samples:.1%} zero fees",
                    "description": "Ensures real fee data is captured"
                },
                {
                    "dimension": "transaction_success",
                    "threshold": f"{self.thresholds.min_success_rate:.1%} success rate",
                    "description": "Ensures representative transaction patterns"
                }
            ],
            "impact": "Artifacts failing quality checks are rejected and not stored"
        }

# Factory function
def get_quality_enforcer(thresholds: QualityThresholds = None) -> QualityEnforcer:
    """Get quality enforcer instance."""
    return QualityEnforcer(thresholds)
