"""
Artifact Builder Service
Transforms collected samples into content-addressed Gas Memory Artifacts.
"""
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from statistics import mean, stdev
import math

from app.models.artifact import (
    GasMemoryArtifact,
    TemporalDatapoint,
    FeeCurve,
    LatencyDistribution,
    SuccessRateStats,
    VerificationProof,
    VerificationMetadata,
    ScopeDefinition,
    SamplesSummary,
    LLMInterpretation,
    CollateralLogic,
    CanonicalBundle
)
from app.models.schemas import FeeSample


class ArtifactBuilder:
    """Builds canonical gas memory artifacts from verified samples."""
    
    def __init__(self):
        self.version = "1.0.0"
    
    def build_artifact(
        self,
        collection_id: str,
        samples: List[FeeSample],
        verification_proofs: List[VerificationProof],
        scope: ScopeDefinition,
        llm_interpretation: Optional[LLMInterpretation] = None,
        raw_samples_cid: Optional[str] = None
    ) -> GasMemoryArtifact:
        """
        Build a complete artifact from verified samples.
        
        Args:
            collection_id: Original collection identifier
            samples: Verified fee samples
            verification_proofs: On-chain verification proofs
            scope: Collection scope definition
            llm_interpretation: Optional LLM-derived interpretation
            raw_samples_cid: CID of raw samples on IPFS/Arweave
            
        Returns:
            Complete GasMemoryArtifact ready for storage
        """
        created_at = datetime.utcnow()
        
        # Build canonical bundle for hashing
        canonical = CanonicalBundle(
            samples=[self._sample_to_dict(s) for s in samples],
            scope=scope,
            created_at=created_at
        )
        canonical_json = canonical.to_canonical_json()
        canonical_sha256 = hashlib.sha256(canonical_json.encode()).hexdigest()
        
        # Build statistics
        fee_curve = self._compute_fee_curve(samples)
        latency_dist = self._compute_latency_distribution(samples)
        success_stats = self._compute_success_rate(samples)
        samples_summary = self._build_samples_summary(samples, verification_proofs)
        
        # Build temporal surface (30 points: 10 real + 10 extrap + 10 predicted)
        temporal_surface = self._build_temporal_surface(
            samples, 
            verification_proofs,
            fee_curve,
            success_stats
        )
        
        # Build verification metadata
        verification = self._build_verification_metadata(
            samples,
            verification_proofs,
            canonical_sha256
        )
        
        # Build collateral logic
        collateral = self._build_collateral_logic(
            samples,
            verification_proofs,
            scope,
            fee_curve,
            success_stats
        )
        
        # Build gateway URLs
        gateway_urls = []
        if raw_samples_cid and raw_samples_cid.startswith("ipfs://"):
            cid = raw_samples_cid.replace("ipfs://", "")
            gateway_urls.extend([
                f"https://gateway.pinata.cloud/ipfs/{cid}",
                f"https://ipfs.io/ipfs/{cid}",
                f"https://cloudflare-ipfs.com/ipfs/{cid}"
            ])
        
        return GasMemoryArtifact(
            artifact_id=f"gm-{uuid.uuid4().hex[:12]}",
            created_at=created_at,
            canonical_sha256=canonical_sha256,
            raw_samples_cid=raw_samples_cid,
            scope=scope,
            temporal_surface=temporal_surface,
            fee_curve=fee_curve,
            latency_distribution=latency_dist,
            samples_summary=samples_summary,
            success_rate_stats=success_stats,
            verification=verification,
            llm_interpretation=llm_interpretation,
            collateral_logic=collateral,
            gateway_urls=gateway_urls
        )
    
    def _sample_to_dict(self, sample: FeeSample) -> Dict[str, Any]:
        """Convert sample to canonical dict."""
        return {
            "signature": sample.signature,
            "slot": sample.slot,
            "timestamp": sample.timestamp.isoformat() if sample.timestamp else None,
            "compute_units_consumed": sample.compute_units_consumed,
            "compute_unit_limit": sample.compute_unit_limit,
            "compute_unit_price_micro_lamports": sample.compute_unit_price_micro_lamports,
            "priority_fee_lamports": sample.priority_fee_lamports,
            "base_fee_lamports": sample.base_fee_lamports,
            "total_fee_lamports": sample.total_fee_lamports,
            "confirmation_latency_slots": sample.confirmation_latency_slots,
            "success": sample.success,
            "program_ids": sample.program_ids,
            "transaction_type": sample.transaction_type
        }
    
    def _compute_fee_curve(self, samples: List[FeeSample]) -> FeeCurve:
        """Compute fee distribution statistics."""
        # Calculate micro-lamports per CU for each sample
        fees_per_cu = []
        for s in samples:
            if s.compute_units_consumed and s.compute_units_consumed > 0:
                fee_per_cu = (s.priority_fee_lamports or 0) * 1_000_000 / s.compute_units_consumed
                fees_per_cu.append(fee_per_cu)
        
        if not fees_per_cu:
            fees_per_cu = [0.0]
        
        sorted_fees = sorted(fees_per_cu)
        n = len(sorted_fees)
        
        def percentile(p: float) -> float:
            idx = int(n * p / 100)
            return sorted_fees[min(idx, n-1)]
        
        mean_val = mean(sorted_fees)
        std_val = stdev(sorted_fees) if len(sorted_fees) > 1 else 0.0
        
        return FeeCurve(
            p50_micro_lamports_per_cu=percentile(50),
            p75_micro_lamports_per_cu=percentile(75),
            p90_micro_lamports_per_cu=percentile(90),
            p95_micro_lamports_per_cu=percentile(95),
            p99_micro_lamports_per_cu=percentile(99),
            min_micro_lamports_per_cu=min(sorted_fees),
            max_micro_lamports_per_cu=max(sorted_fees),
            mean_micro_lamports_per_cu=mean_val,
            std_micro_lamports_per_cu=std_val,
            sample_count=n
        )
    
    def _compute_latency_distribution(self, samples: List[FeeSample]) -> LatencyDistribution:
        """Compute latency statistics."""
        latencies = [s.confirmation_latency_slots or 0 for s in samples if s.confirmation_latency_slots]
        
        if not latencies:
            latencies = [0]
        
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        
        def percentile(p: float) -> float:
            idx = int(n * p / 100)
            return float(sorted_lat[min(idx, n-1)])
        
        return LatencyDistribution(
            p50_slots=percentile(50),
            p75_slots=percentile(75),
            p90_slots=percentile(90),
            mean_slots=mean(sorted_lat),
            std_slots=stdev(sorted_lat) if len(sorted_lat) > 1 else 0.0,
            sample_count=n
        )
    
    def _compute_success_rate(self, samples: List[FeeSample]) -> SuccessRateStats:
        """Compute success rate with Wilson score confidence interval."""
        total = len(samples)
        successful = sum(1 for s in samples if s.success)
        failed = total - successful
        
        if total == 0:
            return SuccessRateStats(
                success_rate=0.0,
                total_samples=0,
                successful_count=0,
                failed_count=0,
                confidence_lower=0.0,
                confidence_upper=0.0
            )
        
        p = successful / total
        
        # Wilson score interval for 95% confidence
        z = 1.96  # 95% CI
        denominator = 1 + z**2 / total
        centre = (p + z**2 / (2 * total)) / denominator
        margin = z * math.sqrt((p * (1 - p) + z**2 / (4 * total)) / total) / denominator
        
        return SuccessRateStats(
            success_rate=p,
            total_samples=total,
            successful_count=successful,
            failed_count=failed,
            confidence_lower=max(0, centre - margin),
            confidence_upper=min(1, centre + margin),
            confidence_level=0.95
        )
    
    def _build_samples_summary(
        self, 
        samples: List[FeeSample],
        proofs: List[VerificationProof]
    ) -> SamplesSummary:
        """Build summary of samples."""
        verified_count = sum(1 for p in proofs if p.verified)
        
        timestamps = [s.timestamp for s in samples if s.timestamp]
        slots = [s.slot for s in samples if s.slot]
        
        avg_fee = mean([s.total_fee_lamports or 0 for s in samples]) if samples else 0
        avg_cu = mean([s.compute_units_consumed or 0 for s in samples]) if samples else 0
        success_rate = mean([1.0 if s.success else 0.0 for s in samples]) if samples else 0
        
        return SamplesSummary(
            total_count=len(samples),
            verified_count=verified_count,
            time_range={
                "start": min(timestamps) if timestamps else datetime.utcnow(),
                "end": max(timestamps) if timestamps else datetime.utcnow()
            },
            slot_range={
                "start": min(slots) if slots else 0,
                "end": max(slots) if slots else 0
            } if slots else None,
            avg_fee_lamports=avg_fee,
            avg_compute_units=avg_cu,
            success_rate=success_rate
        )
    
    def _build_temporal_surface(
        self,
        samples: List[FeeSample],
        proofs: List[VerificationProof],
        fee_curve: FeeCurve,
        success_stats: SuccessRateStats
    ) -> List[TemporalDatapoint]:
        """
        Build 30-point temporal surface:
        - ~10 real (verified samples)
        - ~10 extrapolated (near-history interpolation)
        - ~10 predicted (forward-looking based on trends)
        """
        surface = []
        
        # Sort samples by timestamp
        sorted_samples = sorted(
            [s for s in samples if s.timestamp], 
            key=lambda x: x.timestamp
        )
        
        # Map signatures to proofs for quick lookup
        proof_map = {p.signature: p for p in proofs}
        
        # 1. Real points (verified samples) - up to 10
        real_count = min(10, len(sorted_samples))
        step = max(1, len(sorted_samples) // real_count) if sorted_samples else 1
        
        for i in range(0, len(sorted_samples), step):
            if len(surface) >= 10:
                break
            
            s = sorted_samples[i]
            proof = proof_map.get(s.signature)
            
            # Calculate fee per CU
            fee_per_cu = 0
            if s.compute_units_consumed and s.compute_units_consumed > 0:
                fee_per_cu = (s.priority_fee_lamports or 0) * 1_000_000 / s.compute_units_consumed
            
            surface.append(TemporalDatapoint(
                timestamp=s.timestamp or datetime.utcnow(),
                point_type="real",
                fee_micro_lamports=fee_per_cu,
                compute_units=s.compute_units_consumed,
                latency_slots=s.confirmation_latency_slots,
                success=s.success,
                signature=s.signature,
                slot=s.slot,
                verified=proof.verified if proof else False,
                confirmation_status=proof.confirmation_status if proof else None,
                provider=proof.provider if proof else None,
                regime=self._classify_regime(fee_per_cu, fee_curve),
                confidence_score=1.0 if (proof and proof.verified) else 0.5
            ))
        
        # 2. Extrapolated points (fill gaps in recent history)
        if len(surface) >= 2:
            # Interpolate between real points
            for i in range(len(surface) - 1):
                if len(surface) >= 20:
                    break
                
                p1 = surface[i]
                p2 = surface[i + 1]
                
                # Midpoint interpolation
                mid_time = p1.timestamp + (p2.timestamp - p1.timestamp) / 2
                mid_fee = (p1.fee_micro_lamports + p2.fee_micro_lamports) / 2 if p1.fee_micro_lamports and p2.fee_micro_lamports else None
                
                surface.append(TemporalDatapoint(
                    timestamp=mid_time,
                    point_type="extrapolated",
                    fee_micro_lamports=mid_fee,
                    latency_slots=(p1.latency_slots + p2.latency_slots) // 2 if p1.latency_slots and p2.latency_slots else None,
                    success=True,  # Assume success for extrapolated
                    verified=False,  # Mark as not verified
                    derivation_method="linear_interpolation",
                    regime=self._classify_regime(mid_fee or 0, fee_curve),
                    confidence_score=0.7
                ))
        
        # 3. Predicted points (forward-looking)
        # Simple trend extrapolation
        if len(surface) >= 5:
            recent = [p for p in surface if p.fee_micro_lamports][-5:]
            if recent:
                avg_fee = mean([p.fee_micro_lamports for p in recent if p.fee_micro_lamports])
                trend = (recent[-1].fee_micro_lamports - recent[0].fee_micro_lamports) / len(recent) if len(recent) > 1 else 0
                
                last_time = max([p.timestamp for p in surface])
                
                for i in range(10):
                    if len(surface) >= 30:
                        break
                    
                    pred_time = last_time + timedelta(minutes=5 * (i + 1))
                    pred_fee = max(0, avg_fee + trend * (i + 1))
                    
                    surface.append(TemporalDatapoint(
                        timestamp=pred_time,
                        point_type="predicted",
                        fee_micro_lamports=pred_fee,
                        latency_slots=recent[-1].latency_slots if recent else None,
                        success=True,
                        verified=False,
                        derivation_method="trend_extrapolation",
                        regime=self._classify_regime(pred_fee, fee_curve),
                        confidence_score=max(0.3, 0.8 - i * 0.05)  # Decreasing confidence
                    ))
        
        return surface[:30]  # Ensure exactly 30 or fewer
    
    def _classify_regime(self, fee_per_cu: float, fee_curve: FeeCurve) -> str:
        """Classify market regime based on fee level."""
        if fee_per_cu > fee_curve.p90_micro_lamports_per_cu:
            return "high_congestion"
        elif fee_per_cu > fee_curve.p50_micro_lamports_per_cu:
            return "elevated_activity"
        elif fee_per_cu > fee_curve.p50_micro_lamports_per_cu * 0.5:
            return "normal"
        else:
            return "low_activity"
    
    def _build_verification_metadata(
        self,
        samples: List[FeeSample],
        proofs: List[VerificationProof],
        canonical_sha256: str
    ) -> VerificationMetadata:
        """Build verification metadata."""
        # Hash the proofs for summary verification
        proofs_data = json.dumps([p.model_dump() for p in proofs], sort_keys=True)
        summary_sha256 = hashlib.sha256(proofs_data.encode()).hexdigest()
        
        # Extract provider stats from proofs
        provider_counts = {}
        for p in proofs:
            provider_counts[p.provider] = provider_counts.get(p.provider, 0) + 1
        
        provider_stats = [
            {"name": name, "verifications": count}
            for name, count in provider_counts.items()
        ]
        
        return VerificationMetadata(
            methods=["getSignatureStatuses", "getTransaction"],
            dataset_sha256=canonical_sha256,
            summary_sha256=summary_sha256,
            verification_timestamp=datetime.utcnow(),
            verification_proofs=proofs,
            verified_samples_count=sum(1 for p in proofs if p.verified),
            total_samples_count=len(samples),
            rejected_signatures=0,  # Set by caller
            anomalies_found=0,  # Set by caller
            provider_stats=provider_stats
        )
    
    def _build_collateral_logic(
        self,
        samples: List[FeeSample],
        proofs: List[VerificationProof],
        scope: ScopeDefinition,
        fee_curve: FeeCurve,
        success_stats: SuccessRateStats
    ) -> CollateralLogic:
        """Build collateral logic describing informational collateral properties."""
        verified_ratio = sum(1 for p in proofs if p.verified) / max(len(proofs), 1)
        
        # Certainty score combines verification ratio and confidence
        certainty = verified_ratio * success_stats.confidence_lower
        
        # Parse time window to minutes
        time_minutes = self._parse_time_window(scope.time_window)
        
        return CollateralLogic(
            artifact_type="gas_memory_post",
            version=self.version,
            certainty_score=certainty,
            temporal_coverage_minutes=time_minutes,
            prediction_horizon_minutes=time_minutes * 0.5,  # Predict 50% forward
            recommended_use_cases=[
                "execution_fee_estimation",
                "compute_budget_planning",
                "risk_assessment_for_defi",
                "validator_prioritization_simulation"
            ],
            limitations=[
                "Based on historical data - future conditions may differ",
                "Single tx_family scope - cross-program interactions not covered",
                "Point-in-time snapshot - market conditions change",
                "Predictions are extrapolations - not guarantees"
            ],
            oracle_type="pricing",
            staking_requirement_suggestion="Stake proportional to value at risk * certainty_score",
            slash_conditions=[
                "Artifact proven to be based on fabricated data",
                "Canonical hash mismatch with retrieved content",
                "Verification proofs found to be invalid"
            ]
        )
    
    def _parse_time_window(self, window: str) -> float:
        """Parse time window string to minutes."""
        window = window.lower()
        if window.endswith("h"):
            return float(window[:-1]) * 60
        elif window.endswith("m"):
            return float(window[:-1])
        elif window.endswith("d"):
            return float(window[:-1]) * 24 * 60
        elif window.endswith("s"):
            return float(window[:-1]) / 60
        else:
            return float(window) / 60  # Assume seconds
    
    async def build_arbitrage_artifact(self, opportunity) -> "ArbitrageArtifact":
        """
        Build content-addressed artifact from arbitrage opportunity.
        
        Creates canonical JSON representation with SHA-256 hash
        for independent verification and IPFS storage.
        """
        from app.models.arbitrage import ArbitrageArtifact, ArbitrageMetadata
        
        # Build canonical data (deterministic ordering)
        canonical_data = {
            "scan_id": opportunity.scan_id,
            "token_pair": opportunity.token_pair,
            "spread_bps": opportunity.spread_bps,
            "best_bid_dex": opportunity.best_bid_dex,
            "best_ask_dex": opportunity.best_ask_dex,
            "timestamp": opportunity.timestamp.isoformat(),
            "quotes": [
                {
                    "dex": q.dex,
                    "price": q.price,
                    "amount_in": q.amount_in,
                    "amount_out": q.amount_out,
                    "latency_ms": q.latency_ms
                }
                for q in sorted(opportunity.quotes, key=lambda x: x.dex)
            ]
        }
        
        # Compute canonical SHA-256
        canonical_json = json.dumps(canonical_data, sort_keys=True, separators=(',', ':'))
        canonical_sha256 = hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
        
        # Generate artifact ID
        artifact_id = f"arb-{opportunity.scan_id}"
        
        # Build metadata
        metadata = ArbitrageMetadata(
            scan_timestamp=opportunity.timestamp,
            quotes_collected=len(opportunity.quotes),
            latency_ms_avg=mean([q.latency_ms for q in opportunity.quotes]),
            spread_bps=opportunity.spread_bps,
            best_bid_dex=opportunity.best_bid_dex,
            best_ask_dex=opportunity.best_ask_dex,
            execution_estimate={
                "gross_profit_bps": opportunity.spread_bps,
                "estimated_fees_bps": 30,  # Jupiter + routing fees
                "net_profit_bps": max(0, opportunity.spread_bps - 30),
                "profitable": opportunity.spread_bps > 50
            }
        )
        
        # Create artifact
        artifact = ArbitrageArtifact(
            artifact_id=artifact_id,
            token_pair=opportunity.token_pair,
            spread_bps=opportunity.spread_bps,
            quotes=canonical_data["quotes"],
            metadata=metadata,
            canonical_sha256=canonical_sha256,
            canonical_json=canonical_json,
            created_at=datetime.utcnow()
        )
        
        print(f"[ArbArtifact] Built {artifact_id}")
        print(f"[ArbArtifact] SHA256: {canonical_sha256[:16]}...")
        print(f"[ArbArtifact] Size: {len(canonical_json)} bytes")
        
        return artifact


# Singleton
_builder: Optional[ArtifactBuilder] = None


def get_artifact_builder() -> ArtifactBuilder:
    """Get artifact builder singleton."""
    global _builder
    if _builder is None:
        _builder = ArtifactBuilder()
    return _builder
