"""
RPC Provider Pool for Gas Memory Collateral
Manages multiple Solana RPC providers with failover, parallel fanout, and health tracking.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum
import hashlib
import httpx


class ProviderStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class SignatureCache:
    """LRU cache for signature verification results."""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
    
    def _make_key(self, signature: str) -> str:
        """Create cache key from signature."""
        return hashlib.sha256(signature.encode()).hexdigest()[:32]
    
    def get(self, signature: str) -> Optional[Any]:
        """Get cached result if not expired."""
        key = self._make_key(signature)
        if key in self._cache:
            result, timestamp = self._cache[key]
            if time.time() - timestamp < self._ttl_seconds:
                return result
            else:
                # Expired
                del self._cache[key]
        return None
    
    def set(self, signature: str, result: Any):
        """Cache result with timestamp."""
        # Evict oldest if at capacity (simple FIFO)
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]
        
        key = self._make_key(signature)
        self._cache[key] = (result, time.time())
    
    def clear(self):
        """Clear all cached entries."""
        self._cache.clear()
    
    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)


@dataclass
class ProviderStats:
    """Stats for a single RPC provider."""
    url: str
    name: str
    status: ProviderStatus = ProviderStatus.HEALTHY
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_count: int = 0
    timeout_count: int = 0
    last_error: Optional[str] = None
    last_used: Optional[float] = None
    avg_response_time_ms: float = 0.0
    consecutive_failures: int = 0
    # Exponential backoff tracking
    backoff_until: float = 0.0
    backoff_level: int = 0
    
    def record_success(self, response_time_ms: float):
        """Record successful request."""
        self.total_requests += 1
        self.successful_requests += 1
        self.consecutive_failures = 0
        self.backoff_level = 0
        self.backoff_until = 0.0
        # Update rolling average
        self.avg_response_time_ms = (
            (self.avg_response_time_ms * (self.successful_requests - 1) + response_time_ms)
            / self.successful_requests
        )
        self.last_used = time.time()
    
    def record_failure(self, error_type: str, is_rate_limit: bool = False):
        """Record failed request."""
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_failures += 1
        self.last_error = error_type
        self.last_used = time.time()
        
        if is_rate_limit:
            self.rate_limited_count += 1
            # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s, max 30s
            self.backoff_level = min(self.consecutive_failures, 6)
            backoff_seconds = min(0.5 * (2 ** self.backoff_level), 30)
            self.backoff_until = time.time() + backoff_seconds
        
        # Update status based on consecutive failures
        if self.consecutive_failures >= 5:
            self.status = ProviderStatus.UNHEALTHY
        elif self.consecutive_failures >= 3:
            self.status = ProviderStatus.DEGRADED
    
    def record_timeout(self):
        """Record timeout."""
        self.timeout_count += 1
        self.record_failure("timeout")
    
    def is_available(self) -> bool:
        """Check if provider is available (not in backoff)."""
        if self.status == ProviderStatus.UNHEALTHY:
            return False
        return time.time() >= self.backoff_until
    
    def get_backoff_seconds(self) -> float:
        """Get remaining backoff time."""
        return max(0, self.backoff_until - time.time())


class RPCProviderPool:
    """
    Manages a pool of Solana RPC providers with automatic failover,
    parallel fanout, concurrency control, and signature caching.
    """
    
    # Hard timeout for RPC calls (seconds)
    RPC_TIMEOUT = 3.0
    
    # Max concurrent RPC calls
    MAX_CONCURRENT = 10
    
    def __init__(self):
        self.providers: List[ProviderStats] = []
        self.current_index = 0
        self._client: Optional[httpx.AsyncClient] = None
        self._signature_cache = SignatureCache(max_size=1000, ttl_seconds=300)
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with hard timeout."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.RPC_TIMEOUT)
        return self._client
    
    def add_provider(self, name: str, url: str):
        """Add an RPC provider to the pool."""
        if url and url.strip():
            self.providers.append(ProviderStats(url=url, name=name))
    
    def get_provider_stats(self) -> List[Dict[str, Any]]:
        """Get stats for all providers."""
        return [
            {
                "name": p.name,
                "status": p.status.value,
                "total_requests": p.total_requests,
                "success_rate": p.successful_requests / max(p.total_requests, 1),
                "rate_limited_count": p.rate_limited_count,
                "timeout_count": p.timeout_count,
                "avg_response_time_ms": round(p.avg_response_time_ms, 2),
                "consecutive_failures": p.consecutive_failures,
                "backoff_seconds": round(p.get_backoff_seconds(), 1),
                "is_available": p.is_available()
            }
            for p in self.providers
        ]
    
    def _get_next_available_provider(self) -> Optional[ProviderStats]:
        """Get the next available provider using round-robin."""
        # Try each provider in round-robin order
        checked = 0
        while checked < len(self.providers):
            provider = self.providers[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.providers)
            
            if provider.is_available():
                return provider
            
            checked += 1
        
        # No available providers - return the one with least backoff
        if self.providers:
            return min(self.providers, key=lambda p: p.backoff_until)
        
        return None
    
    async def call_with_failover(
        self, 
        payload: Dict[str, Any],
        max_retries: int = 3,
        retry_delay: float = 0.5
    ) -> Tuple[Optional[Dict], str, Dict[str, Any]]:
        """
        Make RPC call with automatic provider failover.
        
        Returns:
            Tuple of (response_data, provider_name_used, call_metadata)
        """
        if not self.providers:
            return None, "none", {"error": "No providers configured"}
        
        last_error = None
        attempted_providers = []
        
        for attempt in range(max_retries):
            provider = self._get_next_available_provider()
            
            if not provider:
                # All providers in backoff - wait and retry
                await asyncio.sleep(1.0)
                continue
            
            if provider.name in attempted_providers:
                # Already tried this provider, skip to next
                continue
            
            attempted_providers.append(provider.name)
            
            try:
                client = await self._get_client()
                start_time = time.time()
                
                response = await client.post(provider.url, json=payload, timeout=self.RPC_TIMEOUT)
                response_time_ms = (time.time() - start_time) * 1000
                
                data = response.json()
                
                # Check for JSON-RPC errors indicating rate limit
                if "error" in data:
                    error = data["error"]
                    error_code = error.get("code", 0)
                    error_msg = error.get("message", "").lower()
                    
                    is_rate_limit = (
                        error_code == 429 or
                        "rate" in error_msg or
                        "limit" in error_msg or
                        "capacity" in error_msg or
                        "quota" in error_msg
                    )
                    
                    is_server_error = isinstance(error_code, int) and error_code >= 500
                    
                    if is_rate_limit or is_server_error:
                        provider.record_failure(f"rpc_error_{error_code}", is_rate_limit=True)
                        last_error = f"{provider.name}: {error}"
                        
                        if is_rate_limit:
                            print(f"[RPC] {provider.name} rate limited (code {error_code}), trying next provider...")
                        continue
                    else:
                        # Non-retryable error - return it
                        provider.record_success(response_time_ms)
                        return data, provider.name, {
                            "attempts": attempt + 1,
                            "providers_tried": attempted_providers,
                            "response_time_ms": round(response_time_ms, 2)
                        }
                
                # Success
                provider.record_success(response_time_ms)
                
                return data, provider.name, {
                    "attempts": attempt + 1,
                    "providers_tried": attempted_providers,
                    "response_time_ms": round(response_time_ms, 2)
                }
                
            except httpx.TimeoutException:
                provider.record_timeout()
                last_error = f"{provider.name}: timeout"
                print(f"[RPC] {provider.name} timeout, trying next provider...")
                continue
                
            except httpx.NetworkError as e:
                provider.record_failure("network_error")
                last_error = f"{provider.name}: network error - {e}"
                print(f"[RPC] {provider.name} network error, trying next provider...")
                continue
                
            except Exception as e:
                provider.record_failure(f"exception: {type(e).__name__}")
                last_error = f"{provider.name}: {type(e).__name__} - {e}"
                print(f"[RPC] {provider.name} error: {e}, trying next provider...")
                continue
        
        # All retries exhausted
        return None, "none", {
            "error": f"All providers failed after {max_retries} attempts",
            "last_error": last_error,
            "providers_tried": attempted_providers
        }
    
    async def _call_single_provider(
        self,
        provider: ProviderStats,
        payload: Dict[str, Any]
    ) -> Tuple[Optional[Dict], ProviderStats, Optional[float]]:
        """
        Make RPC call to a single provider with timeout and semaphore control.
        
        Returns:
            Tuple of (response_data, provider, response_time_ms or None)
        """
        async with self._semaphore:  # Concurrency control
            try:
                client = await self._get_client()
                start_time = time.time()
                
                response = await asyncio.wait_for(
                    client.post(provider.url, json=payload, timeout=self.RPC_TIMEOUT),
                    timeout=self.RPC_TIMEOUT + 0.5  # Slightly longer than HTTP timeout
                )
                response_time_ms = (time.time() - start_time) * 1000
                
                data = response.json()
                
                # Check for rate limit / server error
                if "error" in data:
                    error = data["error"]
                    error_code = error.get("code", 0)
                    error_msg = error.get("message", "").lower()
                    
                    is_rate_limit = (
                        error_code == 429 or
                        "rate" in error_msg or
                        "limit" in error_msg or
                        "capacity" in error_msg or
                        "quota" in error_msg
                    )
                    is_server_error = isinstance(error_code, int) and error_code >= 500
                    
                    if is_rate_limit or is_server_error:
                        provider.record_failure(f"rpc_error_{error_code}", is_rate_limit=True)
                        return None, provider, None
                
                provider.record_success(response_time_ms)
                return data, provider, response_time_ms
                
            except asyncio.TimeoutError:
                provider.record_timeout()
                return None, provider, None
            except httpx.TimeoutException:
                provider.record_timeout()
                return None, provider, None
            except Exception as e:
                provider.record_failure(f"exception: {type(e).__name__}")
                return None, provider, None
    
    async def call_with_parallel_fanout(
        self,
        payload: Dict[str, Any],
        max_concurrent: int = 3
    ) -> Tuple[Optional[Dict], str, Dict[str, Any]]:
        """
        Query multiple providers in parallel, return first valid response.
        
        This is faster than sequential failover for time-sensitive calls.
        
        Returns:
            Tuple of (response_data, provider_name_used, call_metadata)
        """
        if not self.providers:
            return None, "none", {"error": "No providers configured"}
        
        # Get available providers (sorted by health/performance)
        available = [p for p in self.providers if p.is_available()]
        if not available:
            # All in backoff - use the one with least backoff
            available = [min(self.providers, key=lambda p: p.backoff_until)]
        
        # Limit concurrent attempts
        to_query = available[:max_concurrent]
        
        if len(to_query) == 1:
            # Only one provider - use direct call
            return await self.call_with_failover(payload)
        
        # Create tasks for parallel execution
        tasks = [
            self._call_single_provider(p, payload)
            for p in to_query
        ]
        
        # Wait for first successful result
        pending = set(asyncio.create_task(t) for t in tasks)
        attempted = []
        
        while pending:
            # Wait for first completed task
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=self.RPC_TIMEOUT + 1.0
            )
            
            for task in done:
                try:
                    data, provider, response_time_ms = await task
                    attempted.append(provider.name)
                    
                    if data is not None and "error" not in data:
                        # Success! Cancel remaining tasks
                        for t in pending:
                            t.cancel()
                        
                        return data, provider.name, {
                            "method": "parallel_fanout",
                            "providers_tried": attempted,
                            "response_time_ms": round(response_time_ms, 2) if response_time_ms else None,
                            "winning_provider": provider.name
                        }
                    
                    elif data and "error" in data:
                        # Check if it's a non-retryable error
                        error = data["error"]
                        error_code = error.get("code", 0)
                        if not (isinstance(error_code, int) and error_code >= 500):
                            # Client error - return it
                            for t in pending:
                                t.cancel()
                            return data, provider.name, {
                                "method": "parallel_fanout",
                                "providers_tried": attempted
                            }
                
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    attempted.append("error")
        
        # All failed
        return None, "none", {
            "error": "All parallel provider attempts failed",
            "providers_tried": attempted
        }
    
    async def verify_signatures(
        self, 
        signatures: List[str],
        search_transaction_history: bool = True,
        use_cache: bool = True,
        use_parallel: bool = True
    ) -> Tuple[List[Dict], str, Dict[str, Any]]:
        """
        Verify multiple signatures using getSignatureStatuses with caching and failover.
        
        Args:
            signatures: List of signatures to verify
            search_transaction_history: Whether to search transaction history
            use_cache: Whether to use signature cache
            use_parallel: Whether to use parallel provider fanout
        
        Returns:
            Tuple of (verification_results, provider_used, metadata)
        """
        if not signatures:
            return [], "none", {"error": "No signatures to verify"}
        
        # Check cache for each signature
        cached_results = {}
        uncached_signatures = []
        
        if use_cache:
            for sig in signatures:
                cached = self._signature_cache.get(sig)
                if cached:
                    cached_results[sig] = cached
                else:
                    uncached_signatures.append(sig)
        else:
            uncached_signatures = signatures
        
        # If all cached, return immediately
        if not uncached_signatures:
            verification_results = [
                {
                    "signature": sig,
                    "status": cached_results[sig],
                    "verified": cached_results[sig] is not None,
                    "provider": "cache"
                }
                for sig in signatures
            ]
            return verification_results, "cache", {
                "method": "cache_hit",
                "cached_count": len(signatures),
                "cache_size": self._signature_cache.size()
            }
        
        # Query uncached signatures
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [
                uncached_signatures,
                {"searchTransactionHistory": search_transaction_history}
            ]
        }
        
        # Use parallel fanout for faster response
        if use_parallel and len(self.providers) > 1:
            data, provider_name, metadata = await self.call_with_parallel_fanout(payload)
        else:
            data, provider_name, metadata = await self.call_with_failover(payload)
        
        if data is None:
            # Return cached results + error for uncached
            verification_results = [
                {
                    "signature": sig,
                    "status": cached_results.get(sig),
                    "verified": cached_results.get(sig) is not None,
                    "provider": "cache" if sig in cached_results else "error"
                }
                for sig in signatures
            ]
            return verification_results, provider_name, metadata
        
        if "error" in data:
            return [], provider_name, {**metadata, "rpc_error": data["error"]}
        
        result = data.get("result", {})
        statuses = result.get("value", [])
        
        # Build verification results and cache them
        verification_results = []
        for sig, status in zip(uncached_signatures, statuses):
            result_item = {
                "signature": sig,
                "status": status,
                "verified": status is not None,
                "provider": provider_name
            }
            verification_results.append(result_item)
            
            # Cache the result
            if use_cache:
                self._signature_cache.set(sig, status)
        
        # Add cached results
        for sig in cached_results:
            verification_results.append({
                "signature": sig,
                "status": cached_results[sig],
                "verified": cached_results[sig] is not None,
                "provider": "cache"
            })
        
        # Reorder to match original signatures
        sig_order = {sig: idx for idx, sig in enumerate(signatures)}
        verification_results.sort(key=lambda x: sig_order.get(x["signature"], 999))
        
        metadata["cache_size"] = self._signature_cache.size()
        metadata["cached_hits"] = len(cached_results)
        
        return verification_results, provider_name, metadata
    
    async def get_transaction_with_fallback(
        self, 
        signature: str,
        max_supported_version: int = 0
    ) -> Tuple[Optional[Dict], str, Dict[str, Any]]:
        """
        Get transaction details with fallback across providers.
        
        Returns:
            Tuple of (transaction_data, provider_used, metadata)
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "json",
                    "maxSupportedTransactionVersion": max_supported_version,
                    "commitment": "confirmed"
                }
            ]
        }
        
        return await self.call_with_failover(payload)
    
    async def close(self):
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Singleton instance
_provider_pool: Optional[RPCProviderPool] = None


async def get_provider_pool() -> RPCProviderPool:
    """Get or initialize the provider pool singleton."""
    global _provider_pool
    
    if _provider_pool is None:
        from app.utils.config import settings
        
        _provider_pool = RPCProviderPool()
        
        # Add providers from config
        if settings.ALCHEMY_RPC_URL:
            _provider_pool.add_provider("alchemy", settings.ALCHEMY_RPC_URL)
        
        if settings.HELIUS_RPC_URL:
            _provider_pool.add_provider("helius", settings.HELIUS_RPC_URL)
        
        if settings.QUICKNODE_RPC_URL:
            _provider_pool.add_provider("quicknode", settings.QUICKNODE_RPC_URL)
        
        # Always add public RPC as last resort
        _provider_pool.add_provider("public", "https://api.mainnet-beta.solana.com")
    
    return _provider_pool


async def reset_provider_pool():
    """Reset the provider pool (for testing)."""
    global _provider_pool
    if _provider_pool:
        await _provider_pool.close()
    _provider_pool = None
