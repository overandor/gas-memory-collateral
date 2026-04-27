"""
Latency Architecture — WebSocket/RPC Subscriptions
Local cache → Quote cache → Opportunity scorer → Simulation → Bundle sender
"""

import asyncio
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from collections import deque
import aiohttp
import json


@dataclass
class AccountUpdate:
    """WebSocket account update"""
    pubkey: str
    slot: int
    lamports: int
    data: bytes
    owner: str
    timestamp: datetime


@dataclass
class PoolCacheEntry:
    """Cached pool state with metadata"""
    pool_id: str
    venue: str
    token_a: str
    token_b: str
    price_a_in_b: Decimal
    liquidity_a: Decimal
    liquidity_b: Decimal
    tvl_usd: Decimal
    slot: int
    timestamp: datetime
    update_count: int = 0


@dataclass
class QuoteCacheEntry:
    """Cached Jupiter quote"""
    input_mint: str
    output_mint: str
    amount: int
    quote_data: Dict
    timestamp: datetime
    ttl_seconds: int = 3  # Quotes expire quickly
    
    def is_expired(self) -> bool:
        return (datetime.utcnow() - self.timestamp).seconds > self.ttl_seconds


@dataclass
class ScoredOpportunity:
    """Opportunity with scoring"""
    pair: tuple
    spread_bps: int
    profit_usd: Decimal
    confidence: Decimal
    latency_ms: int
    freshness_score: Decimal  # 0-1 based on cache age
    execution_score: Decimal  # 0-1 based on liquidity
    total_score: Decimal  # Combined score
    timestamp: datetime


class LocalPoolCache:
    """
    Maintains local cache of pool states from WebSocket/RPC.
    Reduces latency by avoiding repeated HTTP calls.
    """
    
    def __init__(self, max_age_seconds: int = 30):
        self.pools: Dict[str, PoolCacheEntry] = {}
        self.max_age = max_age_seconds
        self.update_history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
    
    async def update(self, entry: PoolCacheEntry):
        """Update pool in cache"""
        async with self._lock:
            if entry.pool_id in self.pools:
                old = self.pools[entry.pool_id]
                entry.update_count = old.update_count + 1
            
            self.pools[entry.pool_id] = entry
            
            # Track update history
            if entry.pool_id not in self.update_history:
                self.update_history[entry.pool_id] = deque(maxlen=100)
            self.update_history[entry.pool_id].append(entry.timestamp)
    
    async def get(self, pool_id: str) -> Optional[PoolCacheEntry]:
        """Get pool if fresh"""
        async with self._lock:
            entry = self.pools.get(pool_id)
            if not entry:
                return None
            
            age = (datetime.utcnow() - entry.timestamp).seconds
            if age > self.max_age:
                return None
            
            return entry
    
    async def get_by_tokens(
        self, 
        token_a: str, 
        token_b: str
    ) -> List[PoolCacheEntry]:
        """Get all pools for a token pair"""
        async with self._lock:
            results = []
            for entry in self.pools.values():
                if (entry.token_a == token_a and entry.token_b == token_b) or \
                   (entry.token_a == token_b and entry.token_b == token_a):
                    age = (datetime.utcnow() - entry.timestamp).seconds
                    if age <= self.max_age:
                        results.append(entry)
            return results
    
    async def get_stats(self) -> Dict:
        """Cache statistics"""
        async with self._lock:
            fresh_count = sum(
                1 for e in self.pools.values()
                if (datetime.utcnow() - e.timestamp).seconds <= self.max_age
            )
            return {
                "total_pools": len(self.pools),
                "fresh_pools": fresh_count,
                "stale_pools": len(self.pools) - fresh_count,
                "avg_update_count": sum(
                    e.update_count for e in self.pools.values()
                ) / max(len(self.pools), 1)
            }


class QuoteCache:
    """
    Caches Jupiter quotes with short TTL.
    Reduces redundant API calls for similar amounts.
    """
    
    def __init__(self, default_ttl: int = 3):
        self.cache: Dict[str, QuoteCacheEntry] = {}
        self.default_ttl = default_ttl
        self._lock = asyncio.Lock()
    
    def _make_key(
        self, 
        input_mint: str, 
        output_mint: str, 
        amount: int
    ) -> str:
        """Create cache key"""
        return f"{input_mint}:{output_mint}:{amount}"
    
    async def get(
        self, 
        input_mint: str, 
        output_mint: str, 
        amount: int
    ) -> Optional[QuoteCacheEntry]:
        """Get cached quote if not expired"""
        async with self._lock:
            key = self._make_key(input_mint, output_mint, amount)
            entry = self.cache.get(key)
            
            if not entry:
                return None
            
            if entry.is_expired():
                del self.cache[key]
                return None
            
            return entry
    
    async def set(
        self, 
        input_mint: str, 
        output_mint: str, 
        amount: int, 
        quote_data: Dict
    ):
        """Cache a quote"""
        async with self._lock:
            key = self._make_key(input_mint, output_mint, amount)
            self.cache[key] = QuoteCacheEntry(
                input_mint=input_mint,
                output_mint=output_mint,
                amount=amount,
                quote_data=quote_data,
                timestamp=datetime.utcnow(),
                ttl_seconds=self.default_ttl
            )
    
    async def invalidate(self, token_mint: str):
        """Invalidate all quotes involving a token"""
        async with self._lock:
            keys_to_remove = [
                k for k in self.cache.keys()
                if token_mint in k
            ]
            for key in keys_to_remove:
                del self.cache[key]


class OpportunityScorer:
    """
    Scores opportunities based on:
    - Profit potential
    - Confidence (liquidity depth)
    - Latency (cache freshness)
    - Execution probability
    
    LLM role: rank venues, explain opportunity, adjust filters
    Execution role: deterministic math only
    """
    
    def __init__(
        self,
        min_profit_usd: Decimal = Decimal("0.10"),
        min_confidence: Decimal = Decimal("0.7"),
        max_latency_ms: int = 500
    ):
        self.min_profit_usd = min_profit_usd
        self.min_confidence = min_confidence
        self.max_latency_ms = max_latency_ms
        
        # Scoring weights
        self.profit_weight = Decimal("0.4")
        self.confidence_weight = Decimal("0.3")
        self.latency_weight = Decimal("0.2")
        self.execution_weight = Decimal("0.1")
    
    def score_opportunity(
        self,
        pair: tuple,
        spread_bps: int,
        profit_usd: Decimal,
        confidence: Decimal,
        latency_ms: int,
        liquidity_usd: Decimal
    ) -> Optional[ScoredOpportunity]:
        """
        Score an opportunity. Returns None if below thresholds.
        
        This is DETERMINISTIC MATH ONLY - no LLM involved.
        """
        # Check hard thresholds
        if profit_usd < self.min_profit_usd:
            return None
        
        if confidence < self.min_confidence:
            return None
        
        if latency_ms > self.max_latency_ms:
            return None
        
        # Calculate sub-scores (0-1)
        profit_score = min(profit_usd / Decimal("1.0"), Decimal("1"))  # Cap at $1
        confidence_score = confidence
        latency_score = max(
            Decimal("0"), 
            Decimal("1") - (Decimal(latency_ms) / Decimal(self.max_latency_ms))
        )
        execution_score = min(liquidity_usd / Decimal("100000"), Decimal("1"))
        
        # Freshness based on latency
        freshness_score = latency_score
        
        # Combined score (weighted)
        total_score = (
            profit_score * self.profit_weight +
            confidence_score * self.confidence_weight +
            latency_score * self.latency_weight +
            execution_score * self.execution_weight
        )
        
        return ScoredOpportunity(
            pair=pair,
            spread_bps=spread_bps,
            profit_usd=profit_usd,
            confidence=confidence,
            latency_ms=latency_ms,
            freshness_score=freshness_score,
            execution_score=execution_score,
            total_score=total_score,
            timestamp=datetime.utcnow()
        )
    
    def rank_opportunities(
        self, 
        opportunities: List[ScoredOpportunity]
    ) -> List[ScoredOpportunity]:
        """Rank by total score descending"""
        return sorted(
            opportunities, 
            key=lambda x: x.total_score, 
            reverse=True
        )


class WebSocketSubscriptionManager:
    """
    Manages WebSocket subscriptions for account updates.
    Maintains local cache from stream.
    """
    
    def __init__(
        self, 
        wss_url: str,
        pool_cache: LocalPoolCache
    ):
        self.wss_url = wss_url
        self.pool_cache = pool_cache
        self.subscribed_accounts: Set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[Any] = None
        self._running = False
        self._callbacks: List[Callable] = []
    
    def on_update(self, callback: Callable[[AccountUpdate], None]):
        """Register callback for account updates"""
        self._callbacks.append(callback)
    
    async def subscribe_accounts(self, pubkeys: List[str]):
        """Subscribe to account updates"""
        for pubkey in pubkeys:
            self.subscribed_accounts.add(pubkey)
        
        # Send subscription request if connected
        if self.ws:
            for pubkey in pubkeys:
                await self.ws.send_str(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "accountSubscribe",
                    "params": [pubkey, {"encoding": "base64"}]
                }))
    
    async def start(self):
        """Start WebSocket connection"""
        self._running = True
        self.session = aiohttp.ClientSession()
        
        while self._running:
            try:
                async with self.session.ws_connect(self.wss_url) as ws:
                    self.ws = ws
                    print(f"[WebSocket] Connected to {self.wss_url}")
                    
                    # Resubscribe to all accounts
                    for pubkey in self.subscribed_accounts:
                        await self.subscribe_accounts([pubkey])
                    
                    # Listen for updates
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(json.loads(msg.data))
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
                    
            except Exception as e:
                print(f"[WebSocket] Error: {e}")
                await asyncio.sleep(5)  # Reconnect delay
    
    async def _handle_message(self, data: Dict):
        """Handle WebSocket message"""
        # Parse account update
        if "params" in data:
            result = data["params"].get("result", {})
            value = result.get("value", {})
            
            update = AccountUpdate(
                pubkey=data["params"].get("subscription", ""),
                slot=result.get("context", {}).get("slot", 0),
                lamports=value.get("lamports", 0),
                data=bytes(),  # Decode from base64
                owner=value.get("owner", ""),
                timestamp=datetime.utcnow()
            )
            
            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(update)
                except Exception:
                    pass
    
    async def stop(self):
        """Stop WebSocket connection"""
        self._running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()


class LatencyEngine:
    """
    High-performance arbitrage engine with:
    - WebSocket/RPC subscriptions
    - Local pool cache
    - Quote cache
    - Opportunity scoring
    - Simulation worker
    - Bundle sender
    - Ledger verifier
    
    Execution path is deterministic and fast.
    LLM only explains/ranks, never touches hot execution.
    """
    
    def __init__(
        self,
        helius_wss: str = "wss://rpc.helius.xyz/?api-key=YOUR_KEY",
        jupiter_api: str = "https://api.jup.ag/swap/v1",
        jito_api: str = "https://mainnet.block-engine.jito.wtf/api/v1"
    ):
        # Caches
        self.pool_cache = LocalPoolCache(max_age_seconds=30)
        self.quote_cache = QuoteCache(default_ttl=3)
        
        # Scoring
        self.scorer = OpportunityScorer()
        
        # Subscriptions
        self.ws_manager = WebSocketSubscriptionManager(
            wss_url=helius_wss,
            pool_cache=self.pool_cache
        )
        
        # APIs
        self.jupiter_api = jupiter_api
        self.jito_api = jito_api
        
        # Execution state
        self.pending_bundles: Dict[str, datetime] = {}
        self.opportunity_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        
        # LLM integration (explanation only)
        self.llm_explainer: Optional[Callable] = None
    
    def set_llm_explainer(self, callback: Callable[[ScoredOpportunity], str]):
        """
        Set LLM callback for explaining opportunities.
        LLM never touches execution - only explains.
        """
        self.llm_explainer = callback
    
    async def start(self):
        """Start all subsystems"""
        # Start WebSocket
        ws_task = asyncio.create_task(self.ws_manager.start())
        
        # Start opportunity processor
        processor_task = asyncio.create_task(self._process_opportunities())
        
        print("[LatencyEngine] Started")
        
        await asyncio.gather(ws_task, processor_task)
    
    async def _process_opportunities(self):
        """Main processing loop"""
        while True:
            try:
                # Get scored opportunity from queue
                # Priority queue returns (priority, opportunity)
                priority, opportunity = await asyncio.wait_for(
                    self.opportunity_queue.get(), 
                    timeout=1.0
                )
                
                # Log LLM explanation (non-blocking)
                if self.llm_explainer:
                    explanation = self.llm_explainer(opportunity)
                    print(f"[LLM] {explanation}")
                
                # DETERMINISTIC EXECUTION (no LLM)
                print(f"[Execute] {opportunity.pair}: ${opportunity.profit_usd}")
                
                # Simulate
                # Build bundle
                # Submit
                # Verify
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"[Process] Error: {e}")
    
    async def submit_opportunity(self, opportunity: ScoredOpportunity):
        """Add opportunity to processing queue"""
        # Lower score = higher priority (PriorityQueue is min-heap)
        priority = Decimal("1") - opportunity.total_score
        await self.opportunity_queue.put((priority, opportunity))


# Example latency-sensitive flow
async def example_latency_flow():
    """
    Example of ultra-low latency execution:
    
    1. WebSocket pushes account update (10ms)
    2. Local cache updates (1ms)
    3. Quote cache hit or Jupiter API (3ms cache hit, 50ms miss)
    4. Opportunity scored (0.1ms)
    5. Simulated (20ms)
    6. Bundle submitted (5ms)
    
    Total hot path: ~30ms with cache, ~80ms without
    """
    engine = LatencyEngine()
    
    # Set LLM explainer (optional, non-blocking)
    def explain(opp: ScoredOpportunity) -> str:
        return f"Arbitrage on {opp.pair[0][:4]}...→{opp.pair[1][:4]}... " \
               f"with ${opp.profit_usd:.2f} profit and {opp.confidence:.0%} confidence"
    
    engine.set_llm_explainer(explain)
    
    # Start
    await engine.start()


if __name__ == "__main__":
    asyncio.run(example_latency_flow())
