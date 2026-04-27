"""
Exact Arbitrage Loop — Raydium ↔ Orca
Real implementation using documented endpoints
"""

import asyncio
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import aiohttp


@dataclass
class PoolPrice:
    """Price data from a single DEX pool"""
    dex: str  # 'raydium' or 'orca'
    pool_id: str
    token_a: str
    token_b: str
    token_a_amount: float
    token_b_amount: float
    price_a_in_b: float  # How much token B you get for 1 token A
    price_b_in_a: float
    fee_bps: int
    liquidity_usd: Optional[float]
    timestamp: datetime


@dataclass
class ArbOpportunity:
    """Detected arbitrage opportunity"""
    token_pair: Tuple[str, str]
    buy_dex: str
    sell_dex: str
    buy_pool: str
    sell_pool: str
    buy_price: float
    sell_price: float
    spread_pct: float
    amount_in: float
    expected_profit_usd: float
    confidence: float  # 0-1 based on liquidity depth


class RaydiumOrcaArbitrage:
    """
    Exact arbitrage loop: Raydium ↔ Orca
    
    Flow:
    1. Query Raydium pools
    2. Query Orca pools
    3. Match common pairs
    4. Calculate price differences
    5. Filter by min spread + liquidity
    6. Execute via Jupiter (best route) + Jito bundle
    """
    
    def __init__(
        self,
        min_spread_pct: float = 0.3,  # 0.3% minimum spread
        min_liquidity_usd: float = 10000,  # $10k minimum
        jupiter_api: str = "https://api.jup.ag/swap/v1",
        jito_api: str = "https://mainnet.block-engine.jito.wtf/api/v1",
        helius_rpc: Optional[str] = None
    ):
        self.min_spread_pct = min_spread_pct
        self.min_liquidity_usd = min_liquidity_usd
        self.jupiter_api = jupiter_api
        self.jito_api = jito_api
        self.helius_rpc = helius_rpc
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms between requests
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    # ============================================================
    # STEP 1: Fetch Raydium Pools
    # ============================================================
    
    async def get_raydium_pools(
        self,
        pool_type: str = "all",  # all, concentrated, standard
        page_size: int = 100
    ) -> List[PoolPrice]:
        """
        Fetch Raydium pools from api-v3.raydium.io
        
        Endpoint: GET https://api-v3.raydium.io/pools/info/list
        """
        url = "https://api-v3.raydium.io/pools/info/list"
        params = {
            "poolType": pool_type,
            "poolSortField": "liquidity",
            "sortType": "desc",
            "pageSize": page_size
        }
        
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"Raydium API error: {resp.status}")
                return []
            
            data = await resp.json()
            pools = []
            
            for pool in data.get("data", {}).get("data", []):
                try:
                    # Calculate prices from pool reserves
                    token_a = pool["mintA"]["address"]
                    token_b = pool["mintB"]["address"]
                    
                    # Get decimals for normalization
                    dec_a = pool["mintA"].get("decimals", 9)
                    dec_b = pool["mintB"].get("decimals", 6)
                    
                    # Normalize amounts
                    amount_a = pool["mintAmountA"] / (10 ** dec_a)
                    amount_b = pool["mintAmountB"] / (10 ** dec_b)
                    
                    # Calculate prices (token A in terms of token B)
                    if amount_a > 0:
                        price_a_in_b = amount_b / amount_a
                        price_b_in_a = amount_a / amount_b
                    else:
                        continue
                    
                    pools.append(PoolPrice(
                        dex="raydium",
                        pool_id=pool["id"],
                        token_a=token_a,
                        token_b=token_b,
                        token_a_amount=amount_a,
                        token_b_amount=amount_b,
                        price_a_in_b=price_a_in_b,
                        price_b_in_a=price_b_in_a,
                        fee_bps=pool.get("feeRate", 25),  # 0.25% default
                        liquidity_usd=pool.get("tvl", 0),
                        timestamp=datetime.utcnow()
                    ))
                except Exception:
                    continue
            
            return pools
    
    # ============================================================
    # STEP 2: Fetch Orca Pools
    # ============================================================
    
    async def get_orca_pools(
        self,
        limit: int = 100
    ) -> List[PoolPrice]:
        """
        Fetch Orca Whirlpool pools from api.orca.so
        
        Endpoint: GET https://api.orca.so/v2/solana/whirlpool/list
        """
        url = "https://api.orca.so/v2/solana/whirlpool/list"
        params = {"limit": limit}
        
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"Orca API error: {resp.status}")
                return []
            
            data = await resp.json()
            pools = []
            
            for pool in data.get("data", []):
                try:
                    token_a = pool["tokenA"]["mint"]
                    token_b = pool["tokenB"]["mint"]
                    
                    # Orca gives normalized prices directly (decimals already handled)
                    price_a_in_b = float(pool.get("price", 0))
                    
                    if price_a_in_b <= 0:
                        continue
                    
                    price_b_in_a = 1 / price_a_in_b
                    
                    # Get TVL if available
                    tvl = pool.get("tvl", 0)
                    
                    pools.append(PoolPrice(
                        dex="orca",
                        pool_id=pool["address"],
                        token_a=token_a,
                        token_b=token_b,
                        token_a_amount=float(pool["tokenA"].get("amount", 0)),
                        token_b_amount=float(pool["tokenB"].get("amount", 0)),
                        price_a_in_b=price_a_in_b,
                        price_b_in_a=price_b_in_a,
                        fee_bps=int(pool.get("feeRate", 30) * 100),  # Convert to bps
                        liquidity_usd=tvl,
                        timestamp=datetime.utcnow()
                    ))
                except Exception:
                    continue
            
            return pools
    
    # ============================================================
    # STEP 3: Find Common Pairs & Calculate Spreads
    # ============================================================
    
    def find_arbitrage_opportunities(
        self,
        raydium_pools: List[PoolPrice],
        orca_pools: List[PoolPrice]
    ) -> List[ArbOpportunity]:
        """
        Match pools by token pair and calculate price differences.
        
        Logic:
        - For each pair (token_a, token_b)
        - Compare Raydium price vs Orca price
        - If spread > min_spread_pct → opportunity
        """
        opportunities = []
        
        # Index Orca pools by token pair (sorted tuple)
        orca_index: Dict[Tuple[str, str], List[PoolPrice]] = {}
        for pool in orca_pools:
            pair = tuple(sorted([pool.token_a, pool.token_b]))
            if pair not in orca_index:
                orca_index[pair] = []
            orca_index[pair].append(pool)
        
        # Check each Raydium pool against Orca
        for ray_pool in raydium_pools:
            # Skip low liquidity
            if ray_pool.liquidity_usd and ray_pool.liquidity_usd < self.min_liquidity_usd:
                continue
            
            pair = tuple(sorted([ray_pool.token_a, ray_pool.token_b]))
            
            if pair not in orca_index:
                continue
            
            for orca_pool in orca_index[pair]:
                # Skip low liquidity Orca pools
                if orca_pool.liquidity_usd and orca_pool.liquidity_usd < self.min_liquidity_usd:
                    continue
                
                # Determine direction
                # If Raydium price_a_in_b > Orca price_a_in_b:
                #   Buy A on Orca (cheaper), Sell A on Raydium (higher)
                
                ray_price = ray_pool.price_a_in_b
                orca_price = orca_pool.price_a_in_b
                
                if ray_price > orca_price:
                    # Buy on Orca, Sell on Raydium
                    buy_dex, sell_dex = "orca", "raydium"
                    buy_price, sell_price = orca_price, ray_price
                    buy_pool, sell_pool = orca_pool.pool_id, ray_pool.pool_id
                elif orca_price > ray_price:
                    # Buy on Raydium, Sell on Orca
                    buy_dex, sell_dex = "raydium", "orca"
                    buy_price, sell_price = ray_price, orca_price
                    buy_pool, sell_pool = ray_pool.pool_id, orca_pool.pool_id
                else:
                    continue
                
                # Calculate spread (gross, before fees)
                spread_pct = ((sell_price - buy_price) / buy_price) * 100
                
                if spread_pct < self.min_spread_pct:
                    continue
                
                # Estimate profit (simplified)
                # Assume we trade 10% of smaller pool's liquidity
                min_liquidity = min(
                    ray_pool.liquidity_usd or 0,
                    orca_pool.liquidity_usd or 0
                )
                trade_size = min_liquidity * 0.1  # 10% of smaller pool
                
                # Subtract fees (both pools + Jupiter)
                total_fees_bps = ray_pool.fee_bps + orca_pool.fee_bps + 10  # +0.1% Jupiter
                fee_cost = trade_size * (total_fees_bps / 10000)
                
                gross_profit = trade_size * (spread_pct / 100)
                net_profit = gross_profit - fee_cost
                
                # Confidence score based on liquidity depth
                avg_liquidity = (ray_pool.liquidity_usd + orca_pool.liquidity_usd) / 2
                confidence = min(avg_liquidity / 100000, 1.0)  # Cap at $100k
                
                opportunities.append(ArbOpportunity(
                    token_pair=pair,
                    buy_dex=buy_dex,
                    sell_dex=sell_dex,
                    buy_pool=buy_pool,
                    sell_pool=sell_pool,
                    buy_price=buy_price,
                    sell_price=sell_price,
                    spread_pct=spread_pct,
                    amount_in=trade_size,
                    expected_profit_usd=net_profit,
                    confidence=confidence
                ))
        
        # Sort by expected profit
        opportunities.sort(key=lambda x: x.expected_profit_usd, reverse=True)
        return opportunities
    
    # ============================================================
    # STEP 4: Get Jupiter Quote for Execution
    # ============================================================
    
    async def get_jupiter_execution_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
        slippage_bps: int = 50
    ) -> Optional[Dict]:
        """
        Get execution quote from Jupiter.
        
        Endpoint: GET https://api.jup.ag/swap/v1/quote
        """
        url = f"{self.jupiter_api}/quote"
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": slippage_bps,
            "onlyDirectRoutes": "false"
        }
        
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    
    # ============================================================
    # STEP 5: Execute via Jito Bundle
    # ============================================================
    
    async def execute_arbitrage_bundle(
        self,
        opportunity: ArbOpportunity,
        wallet_pubkey: str,
        jito_tip_lamports: int = 10000
    ) -> Optional[str]:
        """
        Execute arbitrage using Jito bundle for MEV protection.
        
        Bundle structure:
        1. Buy transaction (via Jupiter best route)
        2. Sell transaction (via Jupiter best route)
        3. Jito tip transaction (for bundle inclusion)
        
        Returns bundle UUID if submitted successfully.
        """
        # Note: Full implementation requires wallet signing
        # This shows the structure
        
        print("\n🎯 Executing arbitrage:")
        print(f"   Buy on {opportunity.buy_dex}: ${opportunity.buy_price:.6f}")
        print(f"   Sell on {opportunity.sell_dex}: ${opportunity.sell_price:.6f}")
        print(f"   Expected profit: ${opportunity.expected_profit_usd:.2f}")
        print(f"   Jito tip: {jito_tip_lamports} lamports")
        
        # In real implementation:
        # 1. Build buy tx via Jupiter swap-instructions
        # 2. Build sell tx via Jupiter swap-instructions
        # 3. Add Jito tip tx
        # 4. Sign all transactions
        # 5. Submit bundle to Jito
        
        return "bundle_uuid_placeholder"
    
    # ============================================================
    # MAIN LOOP
    # ============================================================
    
    async def scan_for_opportunities(
        self,
        duration_seconds: int = 60,
        scan_interval_seconds: float = 5.0
    ) -> List[ArbOpportunity]:
        """
        Main scanning loop.
        
        Continuously polls Raydium and Orca, finds opportunities.
        """
        all_opportunities = []
        start_time = datetime.utcnow()
        scan_count = 0
        
        print("🔍 Starting arbitrage scan")
        print(f"   Min spread: {self.min_spread_pct}%")
        print(f"   Min liquidity: ${self.min_liquidity_usd:,.0f}")
        print(f"   Duration: {duration_seconds}s")
        print()
        
        while (datetime.utcnow() - start_time).seconds < duration_seconds:
            scan_count += 1
            print(f"\n--- Scan #{scan_count} ---")
            
            # Fetch pools in parallel
            raydium_task = self.get_raydium_pools(page_size=50)
            orca_task = self.get_orca_pools(limit=50)
            
            raydium_pools, orca_pools = await asyncio.gather(
                raydium_task, orca_task
            )
            
            print(f"   Raydium pools: {len(raydium_pools)}")
            print(f"   Orca pools: {len(orca_pools)}")
            
            # Find opportunities
            opportunities = self.find_arbitrage_opportunities(
                raydium_pools, orca_pools
            )
            
            if opportunities:
                print(f"   🎯 Found {len(opportunities)} opportunities")
                for i, opp in enumerate(opportunities[:3]):
                    print(f"      {i+1}. {opp.buy_dex} → {opp.sell_dex}: "
                          f"{opp.spread_pct:.2f}% spread, "
                          f"${opp.expected_profit_usd:.2f} profit")
                
                all_opportunities.extend(opportunities)
            else:
                print("   No opportunities found")
            
            # Wait before next scan
            if (datetime.utcnow() - start_time).seconds < duration_seconds:
                await asyncio.sleep(scan_interval_seconds)
        
        print(f"\n✅ Scan complete: {scan_count} scans, {len(all_opportunities)} total opportunities")
        return all_opportunities


# ============================================================
# EXACT ENDPOINT MAP (for reference)
# ============================================================

ENDPOINT_MAP = {
    "raydium_pools": "GET https://api-v3.raydium.io/pools/info/list",
    "orca_pools": "GET https://api.orca.so/v2/solana/whirlpool/list",
    "jupiter_quote": "GET https://api.jup.ag/swap/v1/quote",
    "jupiter_swap_instructions": "POST https://api.jup.ag/swap/v1/swap-instructions",
    "jito_bundle": "POST https://mainnet.block-engine.jito.wtf/api/v1/bundles",
    "helius_rpc": "POST https://rpc.helius.xyz/?api-key=YOUR_KEY"
}


# Example usage
async def main():
    """Run the exact arbitrage loop"""
    
    arb = RaydiumOrcaArbitrage(
        min_spread_pct=0.3,  # 0.3% minimum
        min_liquidity_usd=5000  # $5k minimum
    )
    
    async with arb:
        opportunities = await arb.scan_for_opportunities(
            duration_seconds=30,
            scan_interval_seconds=5
        )
        
        if opportunities:
            print("\n🏆 Best opportunity:")
            best = opportunities[0]
            print(f"   Pair: {best.token_pair}")
            print(f"   Buy: {best.buy_dex} @ ${best.buy_price:.6f}")
            print(f"   Sell: {best.sell_dex} @ ${best.sell_price:.6f}")
            print(f"   Spread: {best.spread_pct:.2f}%")
            print(f"   Expected profit: ${best.expected_profit_usd:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
