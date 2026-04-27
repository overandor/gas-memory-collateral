"""
Provenance Chain Tracking for HashDelta System

Tracks the evolution and relationships between artifacts across storage networks.
Creates immutable chains of transformations with cross-network mirroring.
"""
import hashlib
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum

from app.services.hash_delta import Artifact, HashDelta, HashDeltaEngine

class StorageNetwork(Enum):
    """Storage network types."""
    IPFS = "ipfs"
    ARWEAVE = "arweave"
    LOCAL = "local"
    PERMAWEB = "permaweb"

@dataclass
class StorageReference:
    """Reference to artifact in a storage network."""
    network: StorageNetwork
    identifier: str  # CID, TXID, etc.
    gateway_url: Optional[str] = None
    pinned: bool = False
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class ProvenanceLink:
    """Link between two artifacts in the provenance chain."""
    parent_hash: str
    child_hash: str
    transformation_type: str
    delta: HashDelta
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class ProvenanceChain:
    """Complete provenance chain for an artifact lineage."""
    chain_id: str
    root_hash: str
    current_hash: str
    links: List[ProvenanceLink]
    artifacts: Dict[str, Artifact]
    storage_references: Dict[str, List[StorageReference]]
    created_at: datetime
    updated_at: datetime

class ProvenanceChainTracker:
    """
    Tracks and manages provenance chains across storage networks.
    
    Enables:
    - Cross-network mirroring
    - Immutable transformation history
    - Fork detection and resolution
    - Value change tracking
    """
    
    def __init__(self):
        self.chains: Dict[str, ProvenanceChain] = {}
        self.delta_engine = HashDeltaEngine()
    
    def create_chain(
        self,
        initial_artifact: Artifact,
        storage_refs: Optional[List[StorageReference]] = None
    ) -> ProvenanceChain:
        """Create a new provenance chain."""
        chain_id = self._generate_chain_id(initial_artifact.content_hash)
        
        chain = ProvenanceChain(
            chain_id=chain_id,
            root_hash=initial_artifact.content_hash,
            current_hash=initial_artifact.content_hash,
            links=[],
            artifacts={initial_artifact.content_hash: initial_artifact},
            storage_references={},
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Add storage references
        if storage_refs:
            chain.storage_references[initial_artifact.content_hash] = storage_refs
        
        self.chains[chain_id] = chain
        return chain
    
    def add_transformation(
        self,
        chain_id: str,
        parent_artifact: Artifact,
        child_artifact: Artifact,
        storage_refs: Optional[List[StorageReference]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ProvenanceLink:
        """Add a transformation to an existing provenance chain."""
        if chain_id not in self.chains:
            raise ValueError(f"Chain {chain_id} not found")
        
        chain = self.chains[chain_id]
        
        # Verify parent is in chain
        if parent_artifact.content_hash not in chain.artifacts:
            raise ValueError("Parent artifact not found in chain")
        
        # Calculate delta
        delta = self.delta_engine.appraise_transformation(parent_artifact, child_artifact)
        
        # Create provenance link
        link = ProvenanceLink(
            parent_hash=parent_artifact.content_hash,
            child_hash=child_artifact.content_hash,
            transformation_type=delta.transformation_type,
            delta=delta,
            timestamp=datetime.utcnow(),
            metadata=metadata
        )
        
        # Update chain
        chain.links.append(link)
        chain.artifacts[child_artifact.content_hash] = child_artifact
        chain.current_hash = child_artifact.content_hash
        chain.updated_at = datetime.utcnow()
        
        # Add storage references
        if storage_refs:
            chain.storage_references[child_artifact.content_hash] = storage_refs
        
        return link
    
    def add_storage_reference(
        self,
        chain_id: str,
        artifact_hash: str,
        storage_ref: StorageReference
    ):
        """Add a storage reference for an artifact."""
        if chain_id not in self.chains:
            raise ValueError(f"Chain {chain_id} not found")
        
        chain = self.chains[chain_id]
        
        if artifact_hash not in chain.artifacts:
            raise ValueError(f"Artifact {artifact_hash} not found in chain")
        
        if artifact_hash not in chain.storage_references:
            chain.storage_references[artifact_hash] = []
        
        # Check for duplicates
        existing = any(
            ref.network == storage_ref.network and ref.identifier == storage_ref.identifier
            for ref in chain.storage_references[artifact_hash]
        )
        
        if not existing:
            chain.storage_references[artifact_hash].append(storage_ref)
            chain.updated_at = datetime.utcnow()
    
    def get_chain(self, chain_id: str) -> Optional[ProvenanceChain]:
        """Get a provenance chain by ID."""
        return self.chains.get(chain_id)
    
    def get_artifact_history(self, chain_id: str, artifact_hash: str) -> List[ProvenanceLink]:
        """Get the transformation history for a specific artifact."""
        if chain_id not in self.chains:
            return []
        
        chain = self.chains[chain_id]
        history = []
        
        # Find all links leading to this artifact
        for link in chain.links:
            if link.child_hash == artifact_hash:
                history.append(link)
        
        return sorted(history, key=lambda x: x.timestamp)
    
    def verify_chain_integrity(self, chain_id: str) -> Dict[str, Any]:
        """Verify the integrity of a provenance chain."""
        if chain_id not in self.chains:
            return {"valid": False, "error": "Chain not found"}
        
        chain = self.chains[chain_id]
        issues = []
        
        # Check hash continuity
        current_hash = chain.root_hash
        expected_hashes = set()
        
        for link in chain.links:
            if link.parent_hash != current_hash:
                issues.append(f"Broken link at {link.timestamp}: expected {current_hash}, got {link.parent_hash}")
            current_hash = link.child_hash
            expected_hashes.add(link.parent_hash)
            expected_hashes.add(link.child_hash)
        
        # Check final hash
        if current_hash != chain.current_hash:
            issues.append(f"Chain final hash mismatch: expected {current_hash}, got {chain.current_hash}")
        
        # Check all artifacts exist
        for hash_val in expected_hashes:
            if hash_val not in chain.artifacts:
                issues.append(f"Missing artifact: {hash_val}")
        
        # Check for orphaned artifacts
        for hash_val, artifact in chain.artifacts.items():
            if hash_val != chain.root_hash and hash_val != chain.current_hash:
                has_parent = any(link.child_hash == hash_val for link in chain.links)
                has_child = any(link.parent_hash == hash_val for link in chain.links)
                if not (has_parent or has_child):
                    issues.append(f"Orphaned artifact: {hash_val}")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "chain_length": len(chain.links),
            "artifact_count": len(chain.artifacts)
        }
    
    def find_forks(self, chain_id: str) -> List[Dict[str, Any]]:
        """Find forks in the provenance chain."""
        if chain_id not in self.chains:
            return []
        
        chain = self.chains[chain_id]
        forks = []
        
        # Count parents for each hash
        parent_counts = {}
        for link in chain.links:
            if link.child_hash not in parent_counts:
                parent_counts[link.child_hash] = 0
            parent_counts[link.child_hash] += 1
        
        # Find forks (hashes with multiple parents)
        for hash_val, count in parent_counts.items():
            if count > 1:
                parent_links = [link for link in chain.links if link.child_hash == hash_val]
                forks.append({
                    "hash": hash_val,
                    "parent_count": count,
                    "parents": [{"hash": link.parent_hash, "type": link.transformation_type} for link in parent_links],
                    "timestamp": min(link.timestamp for link in parent_links)
                })
        
        return forks
    
    def calculate_value_evolution(self, chain_id: str) -> Dict[str, Any]:
        """Calculate value evolution across the provenance chain."""
        if chain_id not in self.chains:
            return {"error": "Chain not found"}
        
        chain = self.chains[chain_id]
        value_timeline = []
        
        # Start with root artifact
        current_hash = chain.root_hash
        value_score = 0
        
        # Extract value indicators from root
        root_artifact = chain.artifacts[current_hash]
        value_score = self._calculate_artifact_value(root_artifact.content)
        
        value_timeline.append({
            "hash": current_hash,
            "timestamp": root_artifact.timestamp,
            "value_score": value_score,
            "change": 0,
            "change_type": "initial"
        })
        
        # Process each transformation
        for link in chain.links:
            child_artifact = chain.artifacts[link.child_hash]
            new_value_score = self._calculate_artifact_value(child_artifact.content)
            value_change = new_value_score - value_score
            
            value_timeline.append({
                "hash": link.child_hash,
                "timestamp": link.timestamp,
                "value_score": new_value_score,
                "change": value_change,
                "change_type": link.transformation_type,
                "delta_confidence": link.delta.confidence
            })
            
            value_score = new_value_score
        
        return {
            "chain_id": chain_id,
            "initial_value": value_timeline[0]["value_score"],
            "final_value": value_timeline[-1]["value_score"],
            "total_change": value_timeline[-1]["value_score"] - value_timeline[0]["value_score"],
            "timeline": value_timeline
        }
    
    def _calculate_artifact_value(self, content: Dict[str, Any]) -> float:
        """Calculate a numeric value score for an artifact."""
        value_indicators = {
            'ipfs_connected': 2.0,
            'is_real_ipfs': 2.0,
            'pinned': 1.5,
            'storage_type': lambda x: 2.0 if x != 'local_dev_store' else 0.0,
            'verification_proofs': lambda x: 1.0 if x and len(x) > 0 else 0.0,
            'provider_stats': lambda x: 1.0 if x else 0.0,
            'success_rate': lambda x: min(x * 2, 2.0) if isinstance(x, (int, float)) else 0.0,
            'confidence_score': lambda x: min(x * 2, 2.0) if isinstance(x, (int, float)) else 0.0,
            'samples_collected': lambda x: min(x / 1000.0, 1.0) if isinstance(x, (int, float)) else 0.0,
        }
        
        total_value = 0.0
        
        for indicator, evaluator in value_indicators.items():
            if indicator in content:
                try:
                    if callable(evaluator):
                        total_value += evaluator(content[indicator])
                    else:
                        total_value += evaluator if content[indicator] else 0.0
                except (TypeError, ValueError):
                    continue
        
        return total_value
    
    def _generate_chain_id(self, content_hash: str) -> str:
        """Generate a unique chain ID."""
        timestamp = datetime.utcnow().isoformat()
        combined = f"{content_hash}:{timestamp}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    
    def export_chain(self, chain_id: str) -> Optional[Dict[str, Any]]:
        """Export a provenance chain for external use."""
        if chain_id not in self.chains:
            return None
        
        chain = self.chains[chain_id]
        
        return {
            "chain_id": chain.chain_id,
            "root_hash": chain.root_hash,
            "current_hash": chain.current_hash,
            "created_at": chain.created_at.isoformat(),
            "updated_at": chain.updated_at.isoformat(),
            "artifacts": {
                hash_val: asdict(artifact) for hash_val, artifact in chain.artifacts.items()
            },
            "storage_references": {
                hash_val: [asdict(ref) for ref in refs] 
                for hash_val, refs in chain.storage_references.items()
            },
            "links": [asdict(link) for link in chain.links]
        }

# Factory function
def get_provenance_tracker() -> ProvenanceChainTracker:
    """Get provenance chain tracker instance."""
    return ProvenanceChainTracker()
