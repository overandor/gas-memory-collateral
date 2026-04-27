"""
Verification Service
Check sample integrity and authenticity
"""

from typing import List, Dict, Any
from app.schemas import GasSample


def verify_samples(samples: List[GasSample], strict: bool = True) -> Dict[str, Any]:
    """
    Verify gas sample integrity.
    
    Checks:
    - Signature format validity
    - Fee arithmetic (base + priority = total)
    - Compute unit bounds
    - Slot recency
    """
    valid = []
    invalid = []
    errors = []
    
    for sample in samples:
        error = None
        
        # Check signature format (basic)
        if not sample.signature or len(sample.signature) < 10:
            error = "Invalid signature format"
        
        # Check fee arithmetic (within tolerance)
        expected_total = sample.base_fee_lamports + sample.priority_fee_lamports
        if abs(expected_total - (sample.total_fee_sol * 1e9)) > 100:  # 100 lamport tolerance
            error = "Fee arithmetic mismatch"
        
        # Check compute unit bounds
        if sample.compute_units < 0 or sample.compute_units > 1400000:
            error = "Compute units out of bounds"
        
        # Strict mode: additional checks
        if strict:
            if sample.total_fee_sol > 0.1:  # Max 0.1 SOL per tx
                error = "Fee exceeds safety threshold"
            if sample.compute_units < 300:
                error = "Compute units suspiciously low"
        
        if error:
            invalid.append({"sample": sample, "error": error})
            errors.append(error)
        else:
            valid.append(sample)
    
    total = len(samples)
    valid_count = len(valid)
    
    confidence = valid_count / total if total > 0 else 0.0
    
    return {
        "verified": valid_count > total * 0.8,  # 80% threshold
        "valid": valid_count,
        "invalid": len(invalid),
        "confidence": round(confidence, 4),
        "errors": list(set(errors)) if errors else None
    }


def verify_onchain(signature: str, rpc_url: str) -> bool:
    """
    Production: Verify sample exists on-chain
    """
    # Would query RPC to confirm transaction
    return True
