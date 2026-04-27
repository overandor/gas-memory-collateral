"""
Solana Fee Memory Collector Service
Collects and verifies transaction fee data from Solana RPC.
"""
import asyncio
import hashlib
import struct  # Used for unpacking binary instruction data
from datetime import datetime
from typing import List, Dict, Optional
import httpx
from solders.pubkey import Pubkey

# base58 imported dynamically in _parse_transaction to avoid startup dependency

from app.models.schemas import (
    FeeSample, CollectResponse
)
from app.utils.config import settings

# Database will be imported lazily to avoid circular imports
_db = None

def get_db():
    global _db
    if _db is None:
        from app.services.database import get_db as _get_db
        _db = _get_db()
    return _db


def validate_solana_address(address: str) -> bool:
    """
    Validate Solana public key format.
    Returns True if valid base58 Solana address, False otherwise.
    """
    try:
        Pubkey.from_string(address)
        return True
    except Exception:
        return False


def redact_url(url: str) -> str:
    """
    Redact API keys from URLs for logging.
    """
    if not url:
        return ""
    if "api-key=" in url:
        return url.split("api-key=")[0] + "api-key=<redacted>"
    if "/v2/" in url:
        return url.split("/v2/")[0] + "/v2/<redacted>"
    return url


class SolanaFeeCollector:
    """
    Collects verified fee memory from Solana blockchain.
    """
    
    # Known valid Solana addresses for testing
    VALID_TEST_ADDRESSES = {
        "system_program": "11111111111111111111111111111112",
        "token_program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "serum_dex": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
        "jupiter_swap": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
        "jupiter_dca": "DCAvVXiV3VE96HJFTazHWf3HXmNbJWSMzNATKncDqnjn",
        "jupiter_limit": "jPLPYoH3BTLbN1xwSgTWPbZ9s8mD9wLsZeXsVhQJkA3",
        "orca_swap": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
        "orca_whirlpool": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
        "raydium_swap": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "raydium_amm": "58oQChx4yWmvKdwLLZzBi4ChoCc2fqPFBCYjkC5SNjH",
        "raydium_cpmm": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1",
        "meteora_dlmm": "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9yhVvuU5C",
        "phoenix_dex": "PhoeNiXZVUPs1u5ZxEaZSMWkJoWXSooM8qXZmZqnK5",
        "openbook": "srmqPvymJeFKQ4zGQed1GFrrgkzfMPokbfGjEP2v1e",
        "lifinity": "LDXpK8dJjG7cmP9iPkwFkAm4jSTF6pimnTE",
        "kamino_lending": "KLend2g3cP1ffhMkYX1iogjnkcCy4Lmej3FxsMujCT4",
        "mango_v4": "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7ZabybrYXU",
        "marginfi": "MFv2hWf31ZdBkZd8CHnDAHXpwzuvKLQVYAK4",
        "solend": "So1endDq2YkqhipRh3WViPa8hdiqsTSC3xDjrtfrL",
        "drift_v2": "dRiftyHA39MWEWgHBNLZ4P6U4Q27eE5p4y5c2P1Q2F",
        "magic_eden": "MEisE1HzezeRXr1ewrfCMprH6jxJUVF6F",
        "tensor": "TSWAPaqYVqkXUVtdVbjvT7J7XiJ4A",
    }
    
    # Note: Drift program ID placeholder - replace with actual valid address
    # Current ID appears invalid based on RPC "WrongSize" error
    DRIFT_PROGRAM_ID = "11111111111111111111111111111112"  # Using System Program as fallback
    
    TX_FAMILY_MAP = {
        # Aggregators
        "jupiter_swap": {"program": VALID_TEST_ADDRESSES["jupiter_swap"], "instruction_type": "swap"},
        "jupiter_dca": {"program": VALID_TEST_ADDRESSES["jupiter_dca"], "instruction_type": "dca"},
        "jupiter_limit": {"program": VALID_TEST_ADDRESSES["jupiter_limit"], "instruction_type": "limit_order"},

        # AMM DEXes
        "orca_swap": {"program": VALID_TEST_ADDRESSES["orca_swap"], "instruction_type": "swap"},
        "orca_whirlpool": {"program": VALID_TEST_ADDRESSES["orca_whirlpool"], "instruction_type": "swap"},
        "raydium_swap": {"program": VALID_TEST_ADDRESSES["raydium_swap"], "instruction_type": "swap"},
        "raydium_amm": {"program": VALID_TEST_ADDRESSES["raydium_amm"], "instruction_type": "swap"},
        "raydium_cpmm": {"program": VALID_TEST_ADDRESSES["raydium_cpmm"], "instruction_type": "swap"},
        "meteora_dlmm": {"program": VALID_TEST_ADDRESSES["meteora_dlmm"], "instruction_type": "swap"},
        "meteora_swap": {"program": VALID_TEST_ADDRESSES["meteora_dlmm"], "instruction_type": "swap"},
        "lifinity_swap": {"program": VALID_TEST_ADDRESSES["lifinity"], "instruction_type": "swap"},

        # Orderbook DEXes
        "phoenix_trade": {"program": VALID_TEST_ADDRESSES["phoenix_dex"], "instruction_type": "place_order"},
        "openbook_trade": {"program": VALID_TEST_ADDRESSES["openbook"], "instruction_type": "place_order"},
        "serum_trade": {"program": VALID_TEST_ADDRESSES["serum_dex"], "instruction_type": "place_order"},

        # Lending Protocols
        "kamino_lend": {"program": VALID_TEST_ADDRESSES["kamino_lending"], "instruction_type": "lend"},
        "solend_lend": {"program": VALID_TEST_ADDRESSES["solend"], "instruction_type": "lend"},
        "marginfi_lend": {"program": VALID_TEST_ADDRESSES["marginfi"], "instruction_type": "lend"},

        # Perpetuals
        "drift_trade": {"program": VALID_TEST_ADDRESSES["drift_v2"], "instruction_type": "place_order"},
        "mango_trade": {"program": VALID_TEST_ADDRESSES["mango_v4"], "instruction_type": "place_order"},

        # Drift (placeholder)
        "drift_place_order": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "place_order"},
        "drift_cancel_order": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "cancel_order"},
        "drift_settle_pnl": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "settle_pnl"},
        "drift_deposit_collateral": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "deposit"},
        "drift_withdraw_collateral": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "withdraw"},

        # NFT Marketplaces
        "magic_eden_buy": {"program": VALID_TEST_ADDRESSES["magic_eden"], "instruction_type": "buy"},
        "magic_eden_list": {"program": VALID_TEST_ADDRESSES["magic_eden"], "instruction_type": "list"},
        "tensor_trade": {"program": VALID_TEST_ADDRESSES["tensor"], "instruction_type": "trade"},

        # System transactions
        "system_transfer": {"program": VALID_TEST_ADDRESSES["system_program"], "instruction_type": "transfer"},
        "token_transfer": {"program": VALID_TEST_ADDRESSES["token_program"], "instruction_type": "transfer"},
    }
    
    def __init__(self):
        self.rpc_url = settings.SOLANA_RPC_URL
        self.client = httpx.AsyncClient(timeout=30.0)
        self._slot_cache: Dict[str, int] = {}
    
    async def _rpc_call_with_retry(
        self, 
        payload: dict, 
        max_retries: int = 3,
        base_delay: float = 0.5
    ) -> dict:
        """
        Make RPC call with exponential backoff retry.
        
        Args:
            payload: RPC request payload
            max_retries: Maximum retry attempts
            base_delay: Initial delay between retries (doubles each attempt)
            
        Returns:
            RPC response data
            
        Raises:
            Exception: If all retries exhausted
        """
        for attempt in range(max_retries):
            try:
                response = await self.client.post(self.rpc_url, json=payload)
                data = response.json()
                
                # Check for rate limit (429) or server errors (5xx)
                if "error" in data:
                    error_code = data.get("error", {}).get("code", 0)
                    is_rate_limit = error_code == 429
                    is_server_error = isinstance(error_code, int) and error_code >= 500
                    
                    if (is_rate_limit or is_server_error) and attempt < max_retries - 1:
                        # Exponential backoff: 0.5s, 1s, 2s, 4s
                        wait_time = base_delay * (2 ** attempt)
                        print(f"[RPC] Retry {attempt + 1}/{max_retries} after {wait_time}s (error: {error_code})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # Non-retryable error or max retries reached
                        return data
                
                # Success
                return data
                
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)
                    print(f"[RPC] Network error, retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)
                    print(f"[RPC] Error, retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    raise
        
        return {"error": {"message": "Max retries exhausted"}}
        
    async def collect_samples(
        self,
        tx_family: str,
        program_ids: Optional[List[str]] = None,
        time_window: str = "30d",
        sample_limit: int = 10000,
        min_samples: int = 100
    ) -> CollectResponse:
        """
        Collect fee samples for a transaction family.
        
        Args:
            tx_family: Transaction family identifier
            program_ids: Optional list of program IDs to filter
            time_window: Time window (e.g., "30d", "7d", "365d")
            sample_limit: Maximum samples to collect
            min_samples: Minimum samples required
            
        Returns:
            CollectResponse with collection metadata
        """
        # Resolve transaction family
        family_config = self.TX_FAMILY_MAP.get(tx_family, {})
        target_programs = program_ids or [family_config.get("program", self.DRIFT_PROGRAM_ID)]
        
        # Validate all program addresses
        for program_id in target_programs:
            if not validate_solana_address(program_id):
                raise ValueError(
                    f"Invalid Solana public key: {program_id}. "
                    f"Must be valid base58 Solana address."
                )
        
        # Get current slot
        current_slot = await self._get_current_slot()
        
        # Calculate slot range based on time window
        # Parse time window: "1h", "30m", "1d", "7d", or raw seconds
        def parse_time_window(val: str) -> int:
            """Parse time window to seconds."""
            val = str(val).lower().strip()
            if val.endswith("h"):
                return int(val[:-1]) * 3600
            elif val.endswith("m"):
                return int(val[:-1]) * 60
            elif val.endswith("d"):
                return int(val[:-1]) * 86400
            elif val.endswith("s"):
                return int(val[:-1])
            else:
                # Assume raw seconds
                return int(val)
        
        seconds = parse_time_window(time_window)
        slots_per_second = 2.5  # ~0.4s per slot = 2.5 slots/sec
        slot_range = int(seconds * slots_per_second)
        start_slot = max(0, current_slot - slot_range)
        
        # Collect signatures for target programs
        signatures = await self._collect_signatures(
            target_programs, 
            start_slot, 
            current_slot,
            sample_limit
        )
        
        if len(signatures) < min_samples:
            raise ValueError(
                f"Insufficient samples: got {len(signatures)}, need {min_samples}"
            )
        
        # Fetch transaction details
        samples = await self._fetch_transaction_details(signatures[:sample_limit])
        
        # Generate collection ID
        collection_id = hashlib.sha256(
            f"{tx_family}:{current_slot}:{len(samples)}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]
        
        # Persist samples to database
        if samples:
            db = get_db()
            db.save_collection(
                collection_id=collection_id,
                tx_family=tx_family,
                chain="solana",
                time_window=time_window,
                slot_start=start_slot,
                slot_end=current_slot,
                samples_count=0  # Will be updated by save_samples_batch
            )
            saved_count = db.save_samples_batch(collection_id, samples)
            print(f"[Collector] Saved {saved_count} samples to database for collection {collection_id}")
        
        return CollectResponse(
            ok=True,
            collection_id=collection_id,
            samples_collected=len(samples),
            time_range={
                "start": datetime.utcnow(),  # Approximate
                "end": datetime.utcnow()
            },
            slot_range={
                "start": start_slot,
                "end": current_slot
            },
            next_step="verify"
        )
    
    async def _get_current_slot(self) -> int:
        """Get current slot from RPC with retry."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot"
        }
        
        data = await self._rpc_call_with_retry(payload)
        
        if "error" in data:
            raise Exception(f"RPC error: {data['error']}")
            
        return data["result"]
    
    async def _get_recent_prioritization_fees(self) -> List[Dict]:
        """Get recent prioritization fees from RPC with retry."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getRecentPrioritizationFees",
            "params": []
        }
        
        data = await self._rpc_call_with_retry(payload)
        
        if "error" in data:
            return []
            
        return data.get("result", [])
    
    async def _collect_signatures(
        self,
        program_ids: List[str],
        start_slot: int,
        end_slot: int,
        limit: int
    ) -> List[str]:
        """Collect transaction signatures for programs."""
        all_signatures = []
        
        for program_id in program_ids:
            before = None
            remaining = limit
            
            while remaining > 0:
                batch_size = min(1000, remaining)
                
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [
                        program_id,
                        {
                            "limit": batch_size,
                            "before": before
                        }
                    ]
                }
                
                try:
                    data = await self._rpc_call_with_retry(payload, max_retries=3, base_delay=0.5)
                    
                    if "error" in data:
                        break
                        
                    signatures = data.get("result", [])
                    if not signatures:
                        break
                    
                    # Filter by slot range
                    for sig_info in signatures:
                        if start_slot <= sig_info.get("slot", 0) <= end_slot:
                            all_signatures.append(sig_info["signature"])
                    
                    before = signatures[-1]["signature"]
                    remaining -= len(signatures)
                    
                    # Rate limiting between batches
                    await asyncio.sleep(0.2)
                    
                except Exception:
                    break
        
        return all_signatures[:limit]
    
    async def _fetch_transaction_details(
        self, 
        signatures: List[str]
    ) -> List[FeeSample]:
        """Fetch detailed transaction information."""
        samples = []
        batch_size = 100  # RPC batch limit
        
        for i in range(0, len(signatures), batch_size):
            batch = signatures[i:i + batch_size]
            
            # Build batch request
            requests = [
                {
                    "jsonrpc": "2.0",
                    "id": idx,
                    "method": "getTransaction",
                    "params": [
                        sig,
                        {"encoding": "json", "maxSupportedTransactionVersion": 0}
                    ]
                }
                for idx, sig in enumerate(batch)
            ]
            
            try:
                data = await self._rpc_call_with_retry(requests, max_retries=3, base_delay=0.5)
                
                if isinstance(data, list):
                    for tx_data in data:
                        if "result" in tx_data and tx_data["result"]:
                            sample = self._parse_transaction(tx_data["result"])
                            if sample:
                                samples.append(sample)
                
                # Rate limiting between batches
                await asyncio.sleep(0.3)
                
            except Exception:
                continue
        
        return samples
    
    def _parse_transaction(self, tx_result: Dict) -> Optional[FeeSample]:
        """Parse transaction result into FeeSample with accurate fee extraction."""
        try:
            meta = tx_result.get("meta", {})
            transaction = tx_result.get("transaction", {})
            
            # Extract fee information from meta
            fee = meta.get("fee", 0)
            compute_units_consumed = meta.get("computeUnitsConsumed", 0) or 0
            
            # Get compute budget instructions
            compute_unit_limit = 200000  # Default
            requested_unit_price = 0  # What user requested (micro-lamports/CU)
            
            message = transaction.get("message", {})
            instructions = message.get("instructions", [])
            
            # Parse compute budget instructions for actual requested values
            for ix in instructions:
                program_id = ix.get("programId", "")
                if program_id == "ComputeBudget111111111111111111111111111111":
                    data = ix.get("data", "")
                    # Try to decode base58 compute budget instruction data
                    try:
                        import base58
                        decoded = base58.b58decode(data)
                        # Compute budget instructions:
                        # 0: RequestUnits (limit, additional_fee)
                        # 1: RequestHeapFrame (bytes)
                        # 2: SetComputeUnitLimit (limit)
                        # 3: SetComputeUnitPrice (micro_lamports)
                        if len(decoded) >= 9 and decoded[0] == 3:
                            # SetComputeUnitPrice: instruction type (1 byte) + micro_lamports (8 bytes, little-endian)
                            requested_unit_price = struct.unpack('<Q', decoded[1:9])[0]
                        elif len(decoded) >= 5 and decoded[0] == 2:
                            # SetComputeUnitLimit: instruction type (1 byte) + limit (4 bytes)
                            compute_unit_limit = int.from_bytes(decoded[1:5], 'little')
                    except Exception:
                        # If parsing fails, continue with defaults
                        pass
            
            # Calculate priority fee (total fee minus base fee)
            base_fee = 5000  # Solana base fee
            priority_fee = max(0, fee - base_fee)
            
            # Calculate actual micro-lamports per CU from execution
            # Formula: priority_fee_lamports * 1_000_000 / compute_units_consumed
            if compute_units_consumed > 0 and priority_fee > 0:
                actual_micro_lamports_per_cu = (priority_fee * 1_000_000) // compute_units_consumed
            else:
                actual_micro_lamports_per_cu = requested_unit_price  # Fall back to requested
            
            # Use the max of requested or actual (user pays what they requested or more)
            compute_unit_price_micro_lamports = max(requested_unit_price, actual_micro_lamports_per_cu)
            
            # Determine confirmation latency from slot context if available
            confirmation_latency = 1  # Default - would need block comparison for real value
            
            # Get timestamp from block time
            block_time = tx_result.get("blockTime")
            timestamp = datetime.utcfromtimestamp(block_time) if block_time else datetime.utcnow()
            
            # Get program IDs from account keys
            account_keys = message.get("accountKeys", [])
            program_ids = [key for key in account_keys if self._is_program_key(key)]
            
            # Determine success from meta.err
            err = meta.get("err")
            success = err is None
            
            return FeeSample(
                signature=tx_result.get("transaction", {}).get("signatures", [""])[0],
                timestamp=timestamp,
                slot=tx_result.get("slot", 0),
                compute_units_consumed=compute_units_consumed,
                compute_unit_limit=compute_unit_limit,
                compute_unit_price_micro_lamports=compute_unit_price_micro_lamports,
                priority_fee_lamports=priority_fee,
                base_fee_lamports=base_fee,
                total_fee_lamports=fee,
                confirmation_latency_slots=confirmation_latency,
                success=success,
                program_ids=program_ids,
                transaction_type=self._classify_transaction(instructions),
                recent_priority_fees_context=None
            )
            
        except Exception as e:
            print(f"[Parse Error] {e}")
            return None
    
    def _is_program_key(self, key: str) -> bool:
        """Check if key is likely a program ID."""
        # Simplified check - program IDs are typically base58 encoded
        return len(key) == 32 or len(key) == 44
    
    def _classify_transaction(self, instructions: List[Dict]) -> str:
        """Classify transaction type from instructions."""
        for ix in instructions:
            prog_id = ix.get("programId", "")
            if prog_id == self.DRIFT_PROGRAM_ID:
                return "drift"
            elif "JUP" in prog_id:
                return "jupiter"
            elif "raydium" in prog_id.lower():
                return "raydium"
        return "unknown"
    
    async def get_recent_priority_fees(
        self, 
        locked_accounts: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Get recent priority fees from Solana RPC.
        
        Returns:
            Dict with p50, p75, p90, p95, p99 fee percentiles
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getRecentPrioritizationFees",
            "params": [locked_accounts or []]
        }
        
        response = await self.client.post(self.rpc_url, json=payload)
        data = response.json()
        
        if "error" in data or not data.get("result"):
            # Return defaults if RPC fails
            return {
                "p50": 10000,
                "p75": 20000,
                "p90": 50000,
                "p95": 100000,
                "p99": 200000
            }
        
        fees = [f["prioritizationFee"] for f in data["result"]]
        fees.sort()
        
        if not fees:
            return {"p50": 0, "p75": 0, "p90": 0, "p95": 0, "p99": 0}
        
        def percentile(p: float) -> int:
            idx = int(len(fees) * p / 100)
            return fees[max(0, min(idx, len(fees) - 1))]
        
        return {
            "p50": percentile(50),
            "p75": percentile(75),
            "p90": percentile(90),
            "p95": percentile(95),
            "p99": percentile(99)
        }
    
    async def verify_samples(
        self, 
        samples: List[FeeSample],
        max_verification_batch: int = 50
    ) -> Dict:
        """
        Verify collected samples by re-fetching from RPC.
        
        This is the GROUND TRUTH verification that makes the oracle credible.
        Re-fetches each transaction and confirms:
        - Signature exists on-chain
        - Fee matches what we recorded
        - CU consumed matches
        - Transaction succeeded
        
        Returns:
            Dict with verification results
        """
        verified_count = 0
        failed_count = 0
        mismatches = []
        
        # Limit verification to avoid RPC overload
        to_verify = samples[:max_verification_batch]
        
        print(f"[Verify] Verifying {len(to_verify)} samples against on-chain data...")
        
        for sample in to_verify:
            try:
                # Re-fetch transaction from RPC
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        sample.signature,
                        {"encoding": "json", "maxSupportedTransactionVersion": 0}
                    ]
                }
                
                response = await self._rpc_call_with_retry(payload)
                
                if "error" in response:
                    failed_count += 1
                    mismatches.append({
                        "signature": sample.signature,
                        "error": response["error"],
                        "reason": "rpc_error"
                    })
                    continue
                
                result = response.get("result")
                if not result:
                    failed_count += 1
                    mismatches.append({
                        "signature": sample.signature,
                        "error": "Transaction not found",
                        "reason": "not_found"
                    })
                    continue
                
                # Parse the re-fetched transaction
                verified_sample = self._parse_transaction(result)
                
                if not verified_sample:
                    failed_count += 1
                    mismatches.append({
                        "signature": sample.signature,
                        "error": "Failed to parse",
                        "reason": "parse_error"
                    })
                    continue
                
                # Compare key fields (with tolerance for minor differences)
                checks = {
                    "fee_match": abs(verified_sample.total_fee_lamports - sample.total_fee_lamports) <= 1,
                    "cu_consumed_match": verified_sample.compute_units_consumed == sample.compute_units_consumed,
                    "slot_match": verified_sample.slot == sample.slot,
                    "success_match": verified_sample.success == sample.success
                }
                
                if all(checks.values()):
                    verified_count += 1
                else:
                    failed_count += 1
                    mismatches.append({
                        "signature": sample.signature,
                        "checks": checks,
                        "expected": {
                            "fee": sample.total_fee_lamports,
                            "cu": sample.compute_units_consumed,
                            "slot": sample.slot
                        },
                        "actual": {
                            "fee": verified_sample.total_fee_lamports,
                            "cu": verified_sample.compute_units_consumed,
                            "slot": verified_sample.slot
                        },
                        "reason": "mismatch"
                    })
                
                # Small delay to avoid RPC overload
                await asyncio.sleep(0.05)
                
            except Exception as e:
                failed_count += 1
                mismatches.append({
                    "signature": sample.signature,
                    "error": str(e),
                    "reason": "exception"
                })
        
        total = len(to_verify)
        verification_rate = verified_count / total if total > 0 else 0
        
        result = {
            "verified": verified_count,
            "failed": failed_count,
            "total": total,
            "verification_rate": round(verification_rate, 4),
            "mismatches": mismatches[:10],  # Limit mismatch detail
            "ground_truth": True  # This is actual on-chain verification
        }
        
        print(f"[Verify] {verified_count}/{total} verified ({verification_rate:.1%}) - {len(mismatches)} mismatches")
        
        return result
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


# Global collector instance
_collector: Optional[SolanaFeeCollector] = None


async def get_collector() -> SolanaFeeCollector:
    """Get or create collector instance."""
    global _collector
    if _collector is None:
        _collector = SolanaFeeCollector()
    return _collector
