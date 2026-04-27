"""
Commit Service
Record execution decisions with proof
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Optional
from app.schemas import ExecutionDecision


def record_decision(decision: ExecutionDecision, outcome_cid: Optional[str] = None) -> Dict[str, Any]:
    """
    Commit execution decision to memory.
    
    Creates verifiable record with hash.
    """
    # Create commit record
    record = {
        "action": decision.action,
        "target_fee_sol": decision.target_fee_sol,
        "max_slippage_bps": decision.max_slippage_bps,
        "expected_profit_sol": decision.expected_profit_sol,
        "outcome_cid": outcome_cid,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Generate deterministic hash
    content = json.dumps(record, sort_keys=True)
    commit_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
    
    # In production: Store to database or IPFS
    # For demo: Return the hash
    
    return {
        "hash": f"commit-{commit_hash}",
        "timestamp": record["timestamp"],
        "record": record
    }


def verify_commit(commit_hash: str, expected_data: Dict[str, Any]) -> bool:
    """
    Verify a commitment matches expected data.
    """
    content = json.dumps(expected_data, sort_keys=True)
    computed = hashlib.sha256(content.encode()).hexdigest()[:16]
    return commit_hash.endswith(computed)
