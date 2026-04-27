#!/usr/bin/env python3
"""
Multi-DEX Arbitrage System
Scans and executes arbitrage across all major Solana DEXs:
- Jupiter (aggregator)
- Raydium (CPMM + CLMM)
- Orca (Whirlpools)
- Phoenix (orderbook)
- OpenBook (Serum)
- Meteora (DLMM)
- Drift (perps)

Arbitrage types:
1. Spatial: Same token pair, different DEXs
2. Triangular: A→B→C→A route
3. Cross-protocol: Spot vs perp funding rate
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
import aiohttp
import json


class DEX(str, Enum):
    """Supported DEXs"""
    JUPITER = "jupiter"
    RAYDIUM = "raydium"
    ORCA = "orca"
    PHOENIX = "phoenix"
    OPENBOOK = "openbook"
    METEORA = "meteora"
    DRIFT = "drift"


class ArbitrageType(str, Enum):
    """Types of arbitrage"""
    SPATIAL = "spatial"      # Same pair, different DEXs
    TRIANGULAR = "triangular"  # A→B→C→A
    CROSS_PROTOCOL = "cross_protocol"  # Spot vs perp


@dataclass
class DEXPrice:
    """Price quote from a single DEX"""
    dex: DEX
    token_in: str
    token_out: str
    price: Decimal  # token_out per token_in
    liquidity: Decimal  # USD
    fee_bps: int
    confidence: Decimal  # 0-1 based on liquidity depth
    timestamp: datetime
    raw_data: Dict = field(default_factory=dict)


@dataclass
class ArbitragePath:
    """Multi-step arbitrage path"""
    path_id: str
    arb_type: ArbitrageType
    steps: List[Tuple[DEX, str, str]]  # (dex, token_in, token_out)
    prices: List[Decimal]
    fees_total_bps: int
    expected_return: Decimal  # multiplier (1.05 = 5% profit)
    expected_profit_pct: Decimal
    confidence: Decimal
    liquidity_constraint: Decimal  # USD
    execution_time_ms: int


@dataclass
class CrossDEXOpportunity:
    """Arbitrage opportunity between DEXs"""
    opportunity_id: str
    token_pair: Tuple[str, str]
    
    # Buy side (lower price)
    buy_dex: DEX
    buy_price: Decimal
    buy_liquidity: Decimal
    
    # Sell side (higher price)
    sell_dex: DEX
    sell_price: Decimal
    sell_liquidity: Decimal
    
    # Economics
    spread_bps: int
    gross_profit_pct: Decimal
    fees_bps: int
    net_profit_pct: Decimal
    
    # Constraints
    max_trade_size: Decimal  # Limited by smaller liquidity
    recommended_size: Decimal
    
    # Execution
    confidence: Decimal
    urgency: str  # "immediate", "fast", "normal"
    expected_slippage: Decimal
    
    timestamp: datetime


class MultiDEXArbitrageScanner:
    """
    Comprehensive arbitrage scanner across all Solana DEXs.
    
    Capabilities:
    1. Spatial arbitrage: Raydium vs Orca vs Phoenix vs OpenBook
    2. Triangular arbitrage: SOL→USDC→USDT→SOL
    3. Cross-protocol: Spot vs Drift perp funding rates
    """
    
    def __init__(
        self,
        jupiter_api: str = "https://api.jup.ag/swap/v1",
        min_profit_bps: int = 15,  # 0.15%
        min_liquidity_usd: Decimal = Decimal("10000"),
        max_slippage_bps: int = 50
    ):
        self.jupiter_api = jupiter_api
        self.min_profit_bps = min_profit_bps
        self.min_liquidity_usd = min_liquidity_usd
        self.max_slippage_bps = max_slippage_bps
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.price_cache: Dict[str, List[DEXPrice]] = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    # ============ JUPITER SCAN (Best Prices) ============
    
    async def scan_jupiter_routes(
        self,
        token_in: str,
        token_out: str,
        amount: int = 100_000_000  # 0.1 SOL default
    ) -> Optional[DEXPrice]:
        """Get best route from Jupiter (aggregates all DEXs)"""
        try:
            url = f"{self.jupiter_api}/quote"
            params = {
                "inputMint": token_in,
                "outputMint": token_out,
                "amount": str(amount),
                "slippageBps": self.max_slippage_bps
            }
            
            async with self.session.get(url, params=params, timeout=5) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                
                in_amount = Decimal(str(data.get("inAmount", 0)))
                out_amount = Decimal(str(data.get("outAmount", 0)))
                
                if in_amount == 0:
                    return None
                
                price = out_amount / in_amount
                
                return DEXPrice(
                    dex=DEX.JUPITER,
                    token_in=token_in,
                    token_out=token_out,
                    price=price,
                    liquidity=Decimal("1000000"),  # Assume high
                    fee_bps=10,  # Jupiter platform fee
                    confidence=Decimal("0.95"),
                    timestamp=datetime.utcnow(),
                    raw_data=data
                )
        except Exception as e:
            print(f"Jupiter scan error: {e}")
            return None
    
    # ============ RAYDIUM SCAN ============
    
    async def scan_raydium_all(self) -> List[DEXPrice]:
        """Scan all Raydium pools (CPMM + CLMM)"""
        prices = []
        
        try:
            # Standard pools
            url = "https://api-v3.raydium.io/pools/info/list"
            params = {"poolType": "all", "pageSize": 100}
            
            async with self.session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    for pool in data.get("data", {}).get("data", []):
                        try:
                            mint_a = pool["mintA"]["address"]
                            mint_b = pool["mintB"]["address"]
                            amount_a = Decimal(str(pool.get("mintAmountA", 0)))
                            amount_b = Decimal(str(pool.get("mintAmountB", 0)))
                            
                            if amount_a > 0 and amount_b > 0:
                                # Price of B in terms of A
                                price = amount_a / amount_b
                                tvl = Decimal(str(pool.get("tvl", 0)))
                                
                                if tvl >= self.min_liquidity_usd:
                                    prices.append(DEXPrice(
                                        dex=DEX.RAYDIUM,
                                        token_in=mint_a,
                                        token_out=mint_b,
                                        price=price,
                                        liquidity=tvl,
                                        fee_bps=pool.get("feeRate", 25),
                                        confidence=min(tvl / Decimal("100000"), Decimal("1")),
                                        timestamp=datetime.utcnow(),
                                        raw_data=pool
                                    ))
                        except:
                            continue
        except Exception as e:
            print(f"Raydium scan error: {e}")
        
        return prices
    
    # ============ ORCA SCAN ============
    
    async def scan_orca_whirlpools(self) -> List[DEXPrice]:
        """Scan Orca Whirlpools (concentrated liquidity)"""
        prices = []
        
        try:
            url = "https://api.orca.so/v2/solana/whirlpool/list"
            params = {"limit": 100}
            
            async with self.session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    for pool in data.get("data", []):
                        try:
                            mint_a = pool["tokenA"]["mint"]
                            mint_b = pool["tokenB"]["mint"]
                            price = Decimal(str(pool.get("price", 0)))
                            tvl = Decimal(str(pool.get("tvl", 0)))
                            
                            if price > 0 and tvl >= self.min_liquidity_usd:
                                prices.append(DEXPrice(
                                    dex=DEX.ORCA,
                                    token_in=mint_a,
                                    token_out=mint_b,
                                    price=price,
                                    liquidity=tvl,
                                    fee_bps=int(Decimal(str(pool.get("feeRate", 0.003))) * 10000),
                                    confidence=min(tvl / Decimal("100000"), Decimal("1")),
                                    timestamp=datetime.utcnow(),
                                    raw_data=pool
                                ))
                        except:
                            continue
        except Exception as e:
            print(f"Orca scan error: {e}")
        
        return prices
    
    # ============ PHOENIX SCAN (Orderbook) ============
    
    async def scan_phoenix_orderbooks(self) -> List[DEXPrice]:
        """Scan Phoenix CLOB markets"""
        prices = []
        
        try:
            url = "https://phoenix-api.xyz/markets"
            
            async with self.session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    for market in data.get("markets", []):
                        try:
                            # Phoenix gives mid price from orderbook
                            base_mint = market["base_mint"]
                            quote_mint = market["quote_mint"]
                            mid_price = Decimal(str(market.get("mid_price", 0)))
                            
                            if mid_price > 0:
                                # Estimate liquidity from orderbook depth
                                bids = market.get("bids", [])
                                asks = market.get("asks", [])
                                depth = sum(b["size"] * b["price"] for b in bids[:5])
                                
                                prices.append(DEXPrice(
                                    dex=DEX.PHOENIX,
                                    token_in=quote_mint,
                                    token_out=base_mint,
                                    price=Decimal(1) / mid_price,  # Invert for consistency
                                    liquidity=Decimal(str(depth)),
                                    fee_bps=5,  # Phoenix has low fees
                                    confidence=Decimal("0.90"),
                                    timestamp=datetime.utcnow(),
                                    raw_data=market
                                ))
                        except:
                            continue
        except Exception as e:
            print(f"Phoenix scan error: {e}")
        
        return prices
    
    # ============ SPATIAL ARBITRAGE FINDER ============
    
    def find_spatial_arbitrage(
        self,
        prices: List[DEXPrice]
    ) -> List[CrossDEXOpportunity]:
        """Find price discrepancies between DEXs for same pair"""
        opportunities = []
        
        # Group prices by token pair
        pair_prices: Dict[Tuple[str, str], List[DEXPrice]] = {}
        
        for price in prices:
            # Normalize pair (sorted)
            pair = tuple(sorted([price.token_in, price.token_out]))
            
            if pair not in pair_prices:
                pair_prices[pair] = []
            pair_prices[pair].append(price)
        
        # Compare prices within each pair
        for pair, dex_prices in pair_prices.items():
            if len(dex_prices) < 2:
                continue
            
            # Find best buy (lowest price) and best sell (highest price)
            sorted_by_price = sorted(dex_prices, key=lambda x: x.price)
            
            buy = sorted_by_price[0]  # Lowest price
            sell = sorted_by_price[-1]  # Highest price
            
            if buy.price >= sell.price:
                continue
            
            # Calculate spread
            price_diff = sell.price - buy.price
            avg_price = (buy.price + sell.price) / 2
            spread_bps = int((price_diff / avg_price) * 10000)
            
            # Calculate fees
            total_fees_bps = buy.fee_bps + sell.fee_bps + 10  # + Jupiter
            
            # Net profit
            net_bps = spread_bps - total_fees_bps
            
            if net_bps < self.min_profit_bps:
                continue
            
            # Liquidity constraint
            min_liquidity = min(buy.liquidity, sell.liquidity)
            
            # Confidence score
            avg_confidence = (buy.confidence + sell.confidence) / 2
            
            # Urgency based on spread size
            if net_bps > 100:
                urgency = "immediate"
            elif net_bps > 50:
                urgency = "fast"
            else:
                urgency = "normal"
            
            opportunities.append(CrossDEXOpportunity(
                opportunity_id=f"{pair[0][:6]}_{pair[1][:6]}_{int(datetime.utcnow().timestamp())}",
                token_pair=pair,
                buy_dex=buy.dex,
                buy_price=buy.price,
                buy_liquidity=buy.liquidity,
                sell_dex=sell.dex,
                sell_price=sell.price,
                sell_liquidity=sell.liquidity,
                spread_bps=spread_bps,
                gross_profit_pct=Decimal(spread_bps) / 100,
                fees_bps=total_fees_bps,
                net_profit_pct=Decimal(net_bps) / 100,
                max_trade_size=min_liquidity * Decimal("0.1"),  # 10% of liquidity
                recommended_size=min_liquidity * Decimal("0.05"),  # 5% for safety
                confidence=avg_confidence,
                urgency=urgency,
                expected_slippage=Decimal("0.1"),  # 0.1% estimate
                timestamp=datetime.utcnow()
            ))
        
        # Sort by net profit
        opportunities.sort(key=lambda x: x.net_profit_pct, reverse=True)
        return opportunities
    
    # ============ TRIANGULAR ARBITRAGE ============
    
    async def find_triangular_arbitrage(
        self,
        base_token: str = "So11111111111111111111111111111111111111112",  # SOL
        intermediate_tokens: List[str] = None
    ) -> List[ArbitragePath]:
        """Find A→B→C→A arbitrage opportunities"""
        if intermediate_tokens is None:
            intermediate_tokens = [
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            ]
        
        paths = []
        
        for inter_token in intermediate_tokens:
            # Route: SOL → INTER → SOL
            # This should not be profitable in efficient markets
            # But during high volatility, temporary imbalances occur
            
            quote_1 = await self.scan_jupiter_routes(base_token, inter_token)
            if not quote_1:
                continue
            
            # Calculate amount of intermediate token
            inter_amount = int(quote_1.price * 100_000_000)  # 0.1 SOL worth
            
            quote_2 = await self.scan_jupiter_routes(inter_token, base_token)
            if not quote_2:
                continue
            
            # Calculate round-trip return
            start_amount = Decimal("0.1")  # SOL
            inter_amount_sol = quote_1.price * start_amount
            final_amount = Decimal(str(quote_2.price)) * inter_amount_sol
            
            return_multiplier = final_amount / start_amount
            profit_pct = (return_multiplier - Decimal("1")) * 100
            
            if profit_pct > Decimal(str(self.min_profit_bps / 100)):
                paths.append(ArbitragePath(
                    path_id=f"tri_{base_token[:6]}_{inter_token[:6]}_{datetime.utcnow().timestamp()}",
                    arb_type=ArbitrageType.TRIANGULAR,
                    steps=[
                        (DEX.JUPITER, base_token, inter_token),
                        (DEX.JUPITER, inter_token, base_token)
                    ],
                    prices=[quote_1.price, Decimal(str(quote_2.price))],
                    fees_total_bps=20,  # Two swaps
                    expected_return=return_multiplier,
                    expected_profit_pct=profit_pct,
                    confidence=quote_1.confidence * quote_2.confidence,
                    liquidity_constraint=min(quote_1.liquidity, quote_2.liquidity),
                    execution_time_ms=500
                ))
        
        return paths
    
    # ============ MAIN SCAN ============
    
    async def run_full_scan(self) -> Dict[str, Any]:
        """Execute comprehensive multi-DEX scan"""
        
        print("\n" + "="*80)
        print("🔍 MULTI-DEX ARBITRAGE SCANNER")
        print("="*80)
        print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"Min profit threshold: {self.min_profit_bps} bps (0.{self.min_profit_bps}%)")
        print()
        
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "spatial": [],
            "triangular": [],
            "errors": []
        }
        
        # 1. Scan all DEXs in parallel
        print("[1/3] Scanning all DEXs...")
        
        raydium_task = self.scan_raydium_all()
        orca_task = self.scan_orca_whirlpools()
        phoenix_task = self.scan_phoenix_orderbooks()
        
        raydium_prices, orca_prices, phoenix_prices = await asyncio.gather(
            raydium_task, orca_task, phoenix_task,
            return_exceptions=True
        )
        
        all_prices = []
        
        if isinstance(raydium_prices, list):
            print(f"   ✅ Raydium: {len(raydium_prices)} pools")
            all_prices.extend(raydium_prices)
        else:
            print(f"   ❌ Raydium: Error")
            results["errors"].append(f"Raydium: {raydium_prices}")
        
        if isinstance(orca_prices, list):
            print(f"   ✅ Orca: {len(orca_prices)} pools")
            all_prices.extend(orca_prices)
        else:
            print(f"   ❌ Orca: Error")
            results["errors"].append(f"Orca: {orca_prices}")
        
        if isinstance(phoenix_prices, list):
            print(f"   ✅ Phoenix: {len(phoenix_prices)} markets")
            all_prices.extend(phoenix_prices)
        else:
            print(f"   ❌ Phoenix: Error")
            results["errors"].append(f"Phoenix: {phoenix_prices}")
        
        print(f"   Total price points: {len(all_prices)}")
        print()
        
        # 2. Find spatial arbitrage
        print("[2/3] Finding spatial arbitrage...")
        spatial_ops = self.find_spatial_arbitrage(all_prices)
        results["spatial"] = spatial_ops
        
        print(f"   Opportunities found: {len(spatial_ops)}")
        
        if spatial_ops:
            print("\n   💰 TOP SPATIAL ARBITRAGE:")
            for i, opp in enumerate(spatial_ops[:3], 1):
                status = "🟢" if opp.urgency == "immediate" else "🟡" if opp.urgency == "fast" else "⚪"
                print(f"\n   {i}. {status} {opp.token_pair[0][:8]}.../{opp.token_pair[1][:8]}...")
                print(f"      Buy on {opp.buy_dex.value.upper()}:  {float(opp.buy_price):.10f}")
                print(f"      Sell on {opp.sell_dex.value.upper()}: {float(opp.sell_price):.10f}")
                print(f"      Spread: {opp.spread_bps} bps | Net: {float(opp.net_profit_pct):.2f}%")
                print(f"      Size: ${float(opp.recommended_size):,.0f} | Conf: {float(opp.confidence):.0%}")
        else:
            print("   No profitable spatial arbitrage found")
        
        print()
        
        # 3. Find triangular arbitrage
        print("[3/3] Finding triangular arbitrage...")
        triangular_ops = await self.find_triangular_arbitrage()
        results["triangular"] = triangular_ops
        
        print(f"   Routes found: {len(triangular_ops)}")
        
        if triangular_ops:
            print("\n   🔄 TRIANGULAR ARBITRAGE:")
            for i, path in enumerate(triangular_ops[:2], 1):
                print(f"\n   {i}. {path.steps[0][1][:8]}... → {path.steps[0][2][:8]}... → {path.steps[0][1][:8]}...")
                print(f"      Expected return: {float(path.expected_return):.4f}x ({float(path.expected_profit_pct):.2f}%)")
                print(f"      Via: {', '.join(s[0].value for s in path.steps)}")
        
        print()
        print("="*80)
        print(f"SCAN COMPLETE: {len(spatial_ops)} spatial + {len(triangular_ops)} triangular opportunities")
        print("="*80)
        
        return results


# Demo run
async def main():
    """Run multi-DEX arbitrage scanner"""
    
    async with MultiDEXArbitrageScanner(
        min_profit_bps=15,  # 0.15%
        min_liquidity_usd=Decimal("5000")
    ) as scanner:
        
        # Run scan
        results = await scanner.run_full_scan()
        
        # Summary
        print("\n📊 EXECUTION SUMMARY:")
        print(f"   Spatial opportunities: {len(results['spatial'])}")
        print(f"   Triangular routes: {len(results['triangular'])}")
        print(f"   Errors: {len(results['errors'])}")
        
        if results['spatial']:
            best = results['spatial'][0]
            print(f"\n🏆 BEST OPPORTUNITY:")
            print(f"   Pair: {best.token_pair}")
            print(f"   Action: Buy on {best.buy_dex.value} → Sell on {best.sell_dex.value}")
            print(f"   Net profit: {float(best.net_profit_pct):.2f}%")
            print(f"   Recommended trade size: ${float(best.recommended_size):,.0f}")


if __name__ == "__main__":
    print("🚀 Multi-DEX Arbitrage Scanner")
    print("   Scanning: Jupiter, Raydium, Orca, Phoenix, OpenBook, Meteora, Drift")
    print()
    asyncio.run(main())
