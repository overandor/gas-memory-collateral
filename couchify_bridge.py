"""
Couchify Bridge
Connects Gas Memory Collateral to Couchify physical infrastructure
Enables: laptops/couches/bikes as compute + data endpoints
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
import httpx

from app.utils.config import settings


@dataclass
class CouchifyNode:
    """Represents a Couchify physical node."""
    node_id: str
    node_type: str  # "laptop", "desktop", "server", "couch"
    status: str  # "online", "offline", "busy"
    capabilities: List[str]  # ["compute", "storage", "api"]
    location: Optional[str] = None
    last_seen: Optional[datetime] = None
    
    # Resources
    cpu_cores: int = 0
    memory_gb: float = 0.0
    storage_gb: float = 0.0
    
    # Pricing
    price_per_hour_usd: float = 0.0
    price_per_gb_storage_usd: float = 0.0


class CouchifyBridge:
    """
    Bridge between Gas Memory and Couchify infrastructure.
    
    Enables:
    1. Deploy gas-memory API to Couchify nodes
    2. Use Couchify nodes as data collection endpoints
    3. Distribute IPFS storage across physical nodes
    4. Monetize compute + storage + data
    """
    
    def __init__(self):
        self.couchify_api = "http://localhost:3000"  # Couchify API
        self.gas_memory_api = f"http://localhost:{settings.PORT}"
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Node registry
        self.registered_nodes: Dict[str, CouchifyNode] = {}
    
    async def discover_nodes(self) -> List[CouchifyNode]:
        """Discover available Couchify nodes."""
        try:
            r = await self.client.get(f"{self.couchify_api}/api/nodes")
            if r.status_code == 200:
                data = r.json()
                nodes = []
                for n in data.get("nodes", []):
                    node = CouchifyNode(
                        node_id=n["id"],
                        node_type=n.get("type", "unknown"),
                        status=n.get("status", "offline"),
                        capabilities=n.get("capabilities", []),
                        cpu_cores=n.get("resources", {}).get("cpu", 0),
                        memory_gb=n.get("resources", {}).get("memory", 0),
                        storage_gb=n.get("resources", {}).get("storage", 0),
                        price_per_hour_usd=n.get("pricing", {}).get("hourly", 0),
                    )
                    nodes.append(node)
                    self.registered_nodes[node.node_id] = node
                return nodes
        except Exception as e:
            print(f"Couchify discovery failed: {e}")
        return []
    
    async def deploy_to_node(self, node_id: str) -> Dict[str, Any]:
        """
        Deploy gas-memory service to a Couchify node.
        
        Returns deployment info including endpoint URL.
        """
        deployment_spec = {
            "service": "gas-memory-collateral",
            "version": "1.0.0",
            "ports": [8000],
            "env": {
                "SOLANA_RPC_URL": settings.SOLANA_RPC_URL,
                "IPFS_API_URL": "http://localhost:5001",
                "LLM_PROVIDER": settings.LLM_PROVIDER,
                "HF_MODEL_NAME": settings.HF_MODEL_NAME,
            },
            "resources": {
                "cpu": "2",
                "memory": "4G",
                "storage": "10G"
            },
            "command": "python -m app.main"
        }
        
        try:
            r = await self.client.post(
                f"{self.couchify_api}/api/nodes/{node_id}/deploy",
                json=deployment_spec
            )
            if r.status_code == 200:
                return {
                    "deployed": True,
                    "node_id": node_id,
                    "endpoint": f"http://{node_id}.couchify.local:8000",
                    "status": "running"
                }
        except Exception as e:
            print(f"Deployment failed: {e}")
        
        return {"deployed": False, "error": "Deployment failed"}
    
    async def register_as_couchify_endpoint(self) -> Dict[str, Any]:
        """
        Register this gas-memory API as a Couchify service.
        Makes it discoverable and bookable via Couchify.
        """
        service_spec = {
            "id": f"gas-memory-{settings.PORT}",
            "name": "Gas Memory Collateral API",
            "description": "Blockchain execution fee intelligence API",
            "type": "api",
            "endpoint": self.gas_memory_api,
            "pricing": {
                "model": "per_request",
                "per_request_usd": 0.001,
                "per_1k_requests_usd": 0.50
            },
            "capabilities": [
                "fee_estimation",
                "execution_scoring",
                "solana_data",
                "ipfs_storage"
            ],
            "docs_url": f"{self.gas_memory_api}/docs",
            "health_url": f"{self.gas_memory_api}/health"
        }
        
        try:
            r = await self.client.post(
                f"{self.couchify_api}/api/services",
                json=service_spec
            )
            if r.status_code in (200, 201):
                return {
                    "registered": True,
                    "service_id": service_spec["id"],
                    "endpoint": service_spec["endpoint"]
                }
        except Exception as e:
            print(f"Registration failed: {e}")
        
        return {"registered": False}
    
    def calculate_node_value(self, node: CouchifyNode) -> Dict[str, float]:
        """
        Calculate the economic value of a node for gas-memory operations.
        
        Returns value breakdown in USD.
        """
        # Compute value (per hour)
        compute_value = node.cpu_cores * 0.10  # $0.10 per core/hour
        
        # Storage value (per GB/month)
        storage_value = node.storage_gb * 0.02  # $0.02 per GB/month
        
        # Data collection value (based on success rate)
        # Nodes near Solana RPC endpoints = higher value
        data_value = 5.0 if "api" in node.capabilities else 0.0
        
        total_hourly = compute_value + (storage_value / 720)  # Convert monthly to hourly
        
        return {
            "compute_hourly_usd": round(compute_value, 3),
            "storage_hourly_usd": round(storage_value / 720, 4),
            "data_collection_usd": round(data_value, 2),
            "total_hourly_usd": round(total_hourly, 3),
            "total_monthly_usd": round(total_hourly * 720, 2)
        }
    
    async def create_data_collection_job(
        self,
        tx_family: str,
        time_window: str,
        target_nodes: int = 3
    ) -> Dict[str, Any]:
        """
        Create a distributed data collection job across Couchify nodes.
        
        This parallelizes data collection for faster processing.
        """
        nodes = await self.discover_nodes()
        available = [n for n in nodes if n.status == "online"]
        
        if len(available) < target_nodes:
            return {
                "created": False,
                "error": f"Only {len(available)} nodes available, need {target_nodes}"
            }
        
        # Split time window across nodes
        # Example: 30d → 3 nodes × 10d each
        selected = available[:target_nodes]
        
        job_spec = {
            "job_id": f"collect-{tx_family}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "tx_family": tx_family,
            "time_window": time_window,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "node_type": n.node_type,
                    "shard": f"{i+1}-of-{target_nodes}",
                    "value": self.calculate_node_value(n)
                }
                for i, n in enumerate(selected)
            ],
            "parallel": True,
            "estimated_duration": "5-10 minutes",
            "estimated_cost_usd": sum(
                self.calculate_node_value(n)["total_hourly_usd"] * 0.5  # 30 min job
                for n in selected
            )
        }
        
        return {
            "created": True,
            "job": job_spec,
            "nodes_assigned": len(selected)
        }
    
    async def health(self) -> Dict[str, Any]:
        """Check bridge health."""
        status = {
            "couchify_api": "unknown",
            "gas_memory_api": "unknown",
            "registered_nodes": len(self.registered_nodes),
            "overall": False
        }
        
        # Check Couchify
        try:
            r = await self.client.get(f"{self.couchify_api}/health", timeout=5)
            status["couchify_api"] = "connected" if r.status_code == 200 else "unavailable"
        except Exception:
            status["couchify_api"] = "unavailable"
        
        # Check Gas Memory
        try:
            r = await self.client.get(f"{self.gas_memory_api}/health", timeout=5)
            status["gas_memory_api"] = "connected" if r.status_code == 200 else "unavailable"
        except Exception:
            status["gas_memory_api"] = "unavailable"
        
        status["overall"] = (
            status["couchify_api"] == "connected" or
            status["gas_memory_api"] == "connected"
        )
        
        return status
    
    async def close(self):
        await self.client.aclose()


# Global bridge
_bridge: Optional[CouchifyBridge] = None


async def get_couchify_bridge() -> CouchifyBridge:
    """Get or create Couchify bridge."""
    global _bridge
    if _bridge is None:
        _bridge = CouchifyBridge()
    return _bridge
