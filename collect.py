"""
Sample Collection Service
Gather gas/fee samples from blockchain RPC
"""

from typing import List, Dict, Any, Optional
import random
from app.schemas import CollectRequest, GasSample


def gather_samples(req: CollectRequest) -> List[GasSample]:
    """
    Collect gas samples from target chain.
    
    In production: Query RPC for recent transactions
    In demo: Generate realistic synthetic samples
    """
    samples = []
    
    base_fee = 5000  # lamports
    
    for i in range(req.sample_count):
        # Simulate realistic fee variation
        cu = random.randint(30000, 1400000)
        pf = int(random.gauss(50000, 20000))  # priority fee
        pf = max(0, pf)  # non-negative
        
        total_lamports = base_fee + pf
        total_sol = total_lamports / 1e9
        
        sample = GasSample(
            slot=random.randint(300000000, 310000000),
            signature=f"demo_sig_{i}_{random.randint(1000, 9999)}",
            compute_units=cu,
            priority_fee_lamports=pf,
            base_fee_lamports=base_fee,
            total_fee_sol=round(total_sol, 9),
            success=random.random() > 0.05,  # 95% success rate
            program_id=req.program_filter or random.choice([
                "JUP6LkbZbjS1jKKwapdHNy74zc3s6AP7u5AKYwZ6C6V",
                "PhoeNiXZxC9FnLDz1K8zdkK8cC88jrKvJEzvmKRja",
                "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
            ]),
            timestamp=None
        )
        samples.append(sample)
    
    return samples


def fetch_from_rpc(rpc_url: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Production: Fetch real samples from Solana/Ethereum RPC
    """
    # Placeholder for actual RPC implementation
    # Would use solana-py or web3.py
    return []
