"""
Summarization Service
LLM-powered analysis of gas patterns
"""

from typing import List, Dict, Any, Literal
from app.schemas import GasSample, SummaryMetrics


def summarize_samples(samples: List[GasSample], style: Literal["concise", "detailed", "technical"] = "concise") -> SummaryMetrics:
    """
    Compute statistical summary of gas samples.
    
    In production: Could use LLM for narrative generation
    """
    if not samples:
        return SummaryMetrics(
            avg_fee_sol=0.0,
            median_fee_sol=0.0,
            min_fee_sol=0.0,
            max_fee_sol=0.0,
            success_rate=0.0,
            sample_count=0
        )
    
    fees = [s.total_fee_sol for s in samples]
    successes = sum(1 for s in samples if s.success)
    
    sorted_fees = sorted(fees)
    n = len(sorted_fees)
    median = sorted_fees[n // 2] if n % 2 else (sorted_fees[n // 2 - 1] + sorted_fees[n // 2]) / 2
    
    return SummaryMetrics(
        avg_fee_sol=round(sum(fees) / len(fees), 9),
        median_fee_sol=round(median, 9),
        min_fee_sol=round(min(fees), 9),
        max_fee_sol=round(max(fees), 9),
        success_rate=round(successes / len(samples), 4),
        sample_count=len(samples),
        time_span_minutes=estimate_time_span(samples)
    )


def estimate_time_span(samples: List[GasSample]) -> int:
    """Estimate time span of samples based on slot progression"""
    if not samples:
        return 0
    slots = [s.slot for s in samples if s.slot]
    if not slots:
        return 0
    # ~400ms per slot on Solana
    slot_diff = max(slots) - min(slots)
    return int(slot_diff * 0.4 / 60)  # Convert to minutes


def generate_narrative(metrics: SummaryMetrics, style: str) -> str:
    """
    Generate human-readable summary narrative.
    
    Production: Use OpenRouter/OpenAI for this
    """
    if style == "concise":
        return (
            f"Based on {metrics.sample_count} samples: "
            f"avg fee {metrics.avg_fee_sol:.6f} SOL, "
            f"{metrics.success_rate*100:.1f}% success rate."
        )
    elif style == "detailed":
        return (
            f"Analysis of {metrics.sample_count} transactions shows "
            f"average fee of {metrics.avg_fee_sol:.6f} SOL "
            f"(median {metrics.median_fee_sol:.6f}, range {metrics.min_fee_sol:.6f}-{metrics.max_fee_sol:.6f}). "
            f"Success rate: {metrics.success_rate*100:.1f}%."
        )
    else:  # technical
        return str(metrics.model_dump())
