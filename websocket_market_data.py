"""
WebSocket Market Data Service
Subscribes to Solana account changes for <100ms latency market data.
Maintains local pool cache, quote cache, and opportunity scorer.
"""
import asyncio
import json
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import websockets
from websockets.exceptions import ConnectionClosed


@dataclass
class PoolState:
    """Real-time pool state from WebSocket updates."""
    pool_address: str
    token_a_mint: str
    token_b_mint: str
    token_a_amount: int
    token_b_amount: int
    price: float
    slot: int
    timestamp: datetime
    source: str  # "raydium", "orca", "jupiter"
    
    def calculate_price(self) -> float:
        """Calculate spot price from pool reserves."""
        if self.token_b_amount == 0:
            return 0.0
        return self.token_a_amount / self.token_b_amount


@dataclass
class QuoteCacheEntry:
    """Cached quote with TTL."""
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price: float
    timestamp: datetime
    source: str
    ttl_ms: int = 5000  # 5 second default TTL
    
    def is_valid(self) -> bool:
        """Check if quote is still valid (not expired)."""
        age_ms = (datetime.utcnow() - self.timestamp).total_seconds() * 1000
        return age_ms < self.ttl_ms


class WebSocketMarketDataService:
    """
    High-frequency market data via WebSocket.
    
    Architecture:
    - WebSocket/RPC account subscriptions
    - Local pool cache (in-memory)
    - Quote cache (TTL-based)
    - Opportunity scorer (deterministic)
    - Simulation worker (async queue)
    - Bundle sender (Jito integration)
    - Ledger verifier (on-chain confirmation)
    
    Target latency: <100ms from account change to opportunity detection
    """
    
    def __init__(
        self,
        rpc_ws_url: str = "wss://api.mainnet-beta.solana.com",
        helius_ws_url: Optional[str] = None,
        commitment: str = "processed"  # "processed" for speed, "confirmed" for safety
    ):
        self.rpc_ws_url = helius_ws_url or rpc_ws_url
        self.commitment = commitment
        
        # Connection state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._running = False
        
        # Caches
        self.pool_cache: Dict[str, PoolState] = {}  # pool_address -> PoolState
        self.quote_cache: Dict[str, QuoteCacheEntry] = {}  # "tokenA:tokenB" -> Quote
        
        # Subscriptions
        self.subscribed_accounts: Set[str] = set()
        self._subscription_id = 1
        self._subscription_map: Dict[int, str] = {}  # sub_id -> account
        
        # Opportunity detection
        self._opportunity_handlers: List[Callable[[Dict], None]] = []
        self._min_spread_bps = 50
        
        # Performance tracking
        self._latency_samples: List[float] = []
        self._update_count = 0
        
        # Task reference
        self._listener_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            print(f"[WebSocket] Connecting to {self.rpc_ws_url}...")
            self._ws = await websockets.connect(self.rpc_ws_url)
            self._connected = True
            print("[WebSocket] Connected")
            return True
        except Exception as e:
            print(f"[WebSocket] Connection failed: {e}")
            return False
    
    async def disconnect(self):
        """Close WebSocket connection."""
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
            self._connected = False
            print("[WebSocket] Disconnected")
    
    async def subscribe_account(self, account_pubkey: str):
        """Subscribe to account changes."""
        if not self._connected or not self._ws:
            print("[WebSocket] Not connected")
            return
        
        if account_pubkey in self.subscribed_accounts:
            return
        
        sub_id = self._subscription_id
        self._subscription_id += 1
        
        message = {
            "jsonrpc": "2.0",
            "id": sub_id,
            "method": "accountSubscribe",
            "params": [
                account_pubkey,
                {"commitment": self.commitment, "encoding": "jsonParsed"}
            ]
        }
        
        await self._ws.send(json.dumps(message))
        self.subscribed_accounts.add(account_pubkey)
        self._subscription_map[sub_id] = account_pubkey
        
        print(f"[WebSocket] Subscribed to {account_pubkey[:16]}... (id: {sub_id})")
    
    async def subscribe_program(self, program_id: str):
        """Subscribe to all accounts owned by a program."""
        if not self._connected or not self._ws:
            return
        
        sub_id = self._subscription_id
        self._subscription_id += 1
        
        message = {
            "jsonrpc": "2.0",
            "id": sub_id,
            "method": "programSubscribe",
            "params": [
                program_id,
                {"commitment": self.commitment, "encoding": "jsonParsed"}
            ]
        }
        
        await self._ws.send(json.dumps(message))
        self._subscription_map[sub_id] = f"program:{program_id}"
        
        print(f"[WebSocket] Subscribed to program {program_id[:16]}... (id: {sub_id})")
    
    async def _listen(self):
        """Main WebSocket listener loop."""
        self._running = True
        
        while self._running and self._connected and self._ws:
            try:
                message = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=30.0
                )
                
                await self._handle_message(message)
                
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await self._ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": 9999,
                        "method": "ping"
                    }))
                except Exception as e:
                    print(f"[WebSocket] Ping failed: {e}")
                    break
                    
            except ConnectionClosed:
                print("[WebSocket] Connection closed")
                break
            except Exception as e:
                print(f"[WebSocket] Listener error: {e}")
                continue
        
        self._running = False
        self._connected = False
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            
            # Check if it's a notification (subscription update)
            if "method" in data and data["method"] == "accountNotification":
                await self._handle_account_notification(data)
            elif "method" in data and data["method"] == "programNotification":
                await self._handle_program_notification(data)
            elif "result" in data:
                # Subscription confirmation
                sub_id = data.get("id")
                if sub_id in self._subscription_map:
                    print(f"[WebSocket] Subscription {sub_id} confirmed")
            
        except json.JSONDecodeError:
            print(f"[WebSocket] Invalid JSON: {message[:200]}")
        except Exception as e:
            print(f"[WebSocket] Message handling error: {e}")
    
    async def _handle_account_notification(self, data: Dict):
        """Process account update notification."""
        start_time = datetime.utcnow()
        
        params = data.get("params", {})
        result = params.get("result", {})
        value = result.get("value", {})
        
        account_pubkey = self._subscription_map.get(params.get("subscription"), "unknown")
        
        # Extract account data
        lamports = value.get("lamports", 0)
        data_base64 = value.get("data", ["", "base64"])[0]
        owner = value.get("owner", "")
        slot = result.get("context", {}).get("slot", 0)
        
        # Update pool cache if it's a pool account
        if owner in ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]:  # Raydium AMM
            await self._update_raydium_pool(account_pubkey, data_base64, slot)
        elif owner in ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]:  # Orca Whirlpool
            await self._update_orca_pool(account_pubkey, data_base64, slot)
        
        # Track latency
        elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        self._latency_samples.append(elapsed_ms)
        if len(self._latency_samples) > 1000:
            self._latency_samples = self._latency_samples[-1000:]
        
        self._update_count += 1
        
        # Log every 100 updates
        if self._update_count % 100 == 0:
            avg_latency = sum(self._latency_samples[-100:]) / 100
            print(f"[WebSocket] Processed {self._update_count} updates, "
                  f"avg latency: {avg_latency:.1f}ms")
    
    async def _handle_program_notification(self, data: Dict):
        """Process program account update."""
        # Similar to account notification but for program subscriptions
        pass
    
    async def _update_raydium_pool(self, address: str, data_base64: str, slot: int):
        """Update Raydium pool in cache."""
        # Parse Raydium pool data
        # This is a placeholder - real implementation needs pool account layout
        pool = PoolState(
            pool_address=address,
            token_a_mint="unknown",
            token_b_mint="unknown",
            token_a_amount=0,
            token_b_amount=0,
            price=0.0,
            slot=slot,
            timestamp=datetime.utcnow(),
            source="raydium"
        )
        self.pool_cache[address] = pool
        
        # Invalidate related quotes
        await self._invalidate_quotes_for_pool(address)
    
    async def _update_orca_pool(self, address: str, data_base64: str, slot: int):
        """Update Orca pool in cache."""
        pool = PoolState(
            pool_address=address,
            token_a_mint="unknown",
            token_b_mint="unknown",
            token_a_amount=0,
            token_b_amount=0,
            price=0.0,
            slot=slot,
            timestamp=datetime.utcnow(),
            source="orca"
        )
        self.pool_cache[address] = pool
        await self._invalidate_quotes_for_pool(address)
    
    async def _invalidate_quotes_for_pool(self, pool_address: str):
        """Invalidate cached quotes that depend on a pool."""
        # Remove expired quotes
        now = datetime.utcnow()
        expired = [
            key for key, quote in self.quote_cache.items()
            if not quote.is_valid()
        ]
        for key in expired:
            del self.quote_cache[key]
    
    async def start(self):
        """Start WebSocket listener."""
        if not await self.connect():
            return False
        
        self._listener_task = asyncio.create_task(self._listen())
        
        # Subscribe to important pools
        # These would be loaded from a pool registry
        important_pools = [
            # Raydium SOL-USDC pool (placeholder)
            "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYuq2",
        ]
        
        for pool in important_pools:
            await self.subscribe_account(pool)
        
        return True
    
    def get_pool_state(self, pool_address: str) -> Optional[PoolState]:
        """Get cached pool state."""
        return self.pool_cache.get(pool_address)
    
    def get_quote(
        self,
        input_mint: str,
        output_mint: str,
        in_amount: int
    ) -> Optional[QuoteCacheEntry]:
        """Get cached quote if valid."""
        key = f"{input_mint}:{output_mint}:{in_amount}"
        quote = self.quote_cache.get(key)
        
        if quote and quote.is_valid():
            return quote
        
        return None
    
    def cache_quote(self, quote: QuoteCacheEntry):
        """Cache a quote."""
        key = f"{quote.input_mint}:{quote.output_mint}:{quote.in_amount}"
        self.quote_cache[key] = quote
    
    def get_latency_stats(self) -> Dict[str, float]:
        """Get WebSocket processing latency statistics."""
        if not self._latency_samples:
            return {"avg_ms": 0, "min_ms": 0, "max_ms": 0, "samples": 0}
        
        recent = self._latency_samples[-100:]
        return {
            "avg_ms": sum(recent) / len(recent),
            "min_ms": min(recent),
            "max_ms": max(recent),
            "samples": len(self._latency_samples)
        }
    
    def register_opportunity_handler(self, handler: Callable[[Dict], None]):
        """Register callback for opportunity detection."""
        self._opportunity_handlers.append(handler)
    
    async def _notify_opportunity(self, opportunity: Dict):
        """Notify all registered handlers of an opportunity."""
        for handler in self._opportunity_handlers:
            try:
                handler(opportunity)
            except Exception as e:
                print(f"[WebSocket] Handler error: {e}")


# Singleton
_ws_service: Optional[WebSocketMarketDataService] = None


def get_websocket_service(
    rpc_ws_url: str = "wss://api.mainnet-beta.solana.com"
) -> WebSocketMarketDataService:
    """Get WebSocket service singleton."""
    global _ws_service
    if _ws_service is None:
        _ws_service = WebSocketMarketDataService(rpc_ws_url)
    return _ws_service
