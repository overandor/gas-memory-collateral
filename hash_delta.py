"""
HashDelta - Artifact Transformation Appraisal Engine

Measures how meaning changes while provenance stays machine-verifiable.

Core primitive:
Text A → canonical JSON → SHA-256 A
Text B → canonical JSON → SHA-256 B
Then compare deltas across multiple dimensions.
"""
import json
import hashlib
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import difflib

@dataclass
class Artifact:
    """Represents an artifact with its hash and metadata."""
    content: Dict[str, Any]
    content_hash: str
    content_type: str
    timestamp: datetime
    source: str
    storage_refs: Optional[Dict[str, str]] = None

@dataclass
class HashDelta:
    """Represents the delta between two artifacts."""
    before_hash: str
    after_hash: str
    hash_visual_similarity: float
    text_semantic_delta: float
    structural_delta: float
    value_delta: str
    verdict: str
    transformation_type: str
    confidence: float

class CanonicalJSON:
    """Canonical JSON serialization for consistent hashing."""
    
    @staticmethod
    def serialize(data: Dict[str, Any]) -> str:
        """
        Serialize data to canonical JSON.
        
        Ensures consistent ordering and formatting for hash stability.
        """
        def canonicalize(obj):
            if isinstance(obj, dict):
                # Sort keys recursively
                return {k: canonicalize(obj[k]) for k in sorted(obj.keys())}
            elif isinstance(obj, list):
                return [canonicalize(item) for item in obj]
            elif isinstance(obj, (str, int, float, bool)) or obj is None:
                return obj
            else:
                # Convert other types to string representation
                return str(obj)
        
        canonical_data = canonicalize(data)
        return json.dumps(canonical_data, separators=(',', ':'), ensure_ascii=False)
    
    @staticmethod
    def hash_content(content: Dict[str, Any]) -> str:
        """Generate SHA-256 hash of canonical JSON."""
        canonical_json = CanonicalJSON.serialize(content)
        return hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

class VisualHashScorer:
    """Scores visual similarity between hash strings."""
    
    @staticmethod
    def hash_visual_similarity(hash1: str, hash2: str) -> float:
        """
        Calculate visual similarity between two hash strings.
        
        Uses character-level similarity and pattern matching.
        Returns 0.0 (completely different) to 1.0 (identical).
        """
        if hash1 == hash2:
            return 1.0
        
        if len(hash1) != len(hash2):
            return 0.0
        
        # Character-level similarity
        char_similarity = sum(c1 == c2 for c1, c2 in zip(hash1, hash2)) / len(hash1)
        
        # Pattern similarity (runs of same character)
        def get_patterns(hash_str):
            patterns = []
            current_char = hash_str[0]
            run_length = 1
            
            for char in hash_str[1:]:
                if char == current_char:
                    run_length += 1
                else:
                    patterns.append((current_char, run_length))
                    current_char = char
                    run_length = 1
            patterns.append((current_char, run_length))
            return patterns
        
        patterns1 = get_patterns(hash1)
        patterns2 = get_patterns(hash2)
        
        # Pattern similarity using sequence matching
        pattern_strings1 = [f"{char}{length}" for char, length in patterns1]
        pattern_strings2 = [f"{char}{length}" for char, length in patterns2]
        
        pattern_similarity = difflib.SequenceMatcher(None, pattern_strings1, pattern_strings2).ratio()
        
        # Weighted combination
        return (char_similarity * 0.7 + pattern_similarity * 0.3)

class SemanticDeltaAnalyzer:
    """Analyzes semantic differences between artifacts."""
    
    @staticmethod
    def calculate_semantic_delta(before: Dict[str, Any], after: Dict[str, Any]) -> float:
        """
        Calculate semantic delta between two artifacts.
        
        Returns 0.0 (identical meaning) to 1.0 (completely different).
        """
        # Extract key fields for semantic comparison
        def extract_semantic_fields(data: Dict[str, Any]) -> Dict[str, Any]:
            semantic = {}
            
            # Common semantic fields
            semantic_fields = [
                'type', 'version', 'created_at', 'status', 'success_rate',
                'confidence_score', 'risk_assessment', 'samples_collected',
                'verification_hash', 'claim', 'provider', 'storage_type'
            ]
            
            for field in semantic_fields:
                if field in data:
                    semantic[field] = data[field]
            
            # Nested fields
            nested_fields = ['samples_summary', 'fee_curve', 'llm_interpretation']
            for field in nested_fields:
                if field in data:
                    semantic[field] = data[field]
            
            return semantic
        
        before_semantic = extract_semantic_fields(before)
        after_semantic = extract_semantic_fields(after)
        
        # Calculate field-level differences
        all_keys = set(before_semantic.keys()) | set(after_semantic.keys())
        
        if not all_keys:
            return 0.0
        
        differences = 0
        total_weight = 0
        
        for key in all_keys:
            before_val = before_semantic.get(key)
            after_val = after_semantic.get(key)
            
            # Weight important fields more heavily
            weight = 2.0 if key in ['type', 'status', 'success_rate', 'confidence_score'] else 1.0
            
            if before_val != after_val:
                if isinstance(before_val, (int, float)) and isinstance(after_val, (int, float)):
                    # Numeric difference
                    if before_val == 0:
                        diff = abs(after_val)
                    else:
                        diff = abs(after_val - before_val) / abs(before_val)
                    differences += min(diff, 1.0) * weight
                elif isinstance(before_val, str) and isinstance(after_val, str):
                    # String similarity
                    similarity = difflib.SequenceMatcher(None, before_val, after_val).ratio()
                    differences += (1.0 - similarity) * weight
                else:
                    # Type or structural difference
                    differences += 1.0 * weight
            
            total_weight += weight
        
        return min(differences / total_weight, 1.0)

class StructuralDeltaAnalyzer:
    """Analyzes structural differences between artifacts."""
    
    @staticmethod
    def calculate_structural_delta(before: Dict[str, Any], after: Dict[str, Any]) -> float:
        """
        Calculate structural delta between two artifacts.
        
        Returns 0.0 (identical structure) to 1.0 (completely different).
        """
        def get_structure(data: Any) -> str:
            """Get structural signature of data."""
            if isinstance(data, dict):
                return f"dict{{{','.join(sorted([f'{k}:{get_structure(v)}' for k, v in data.items()]))}}}"
            elif isinstance(data, list):
                if not data:
                    return "list[]"
                # For lists, check if all elements have same structure
                first_structure = get_structure(data[0])
                if all(get_structure(item) == first_structure for item in data):
                    return f"list[{first_structure}]"
                else:
                    return "list[mixed]"
            else:
                return type(data).__name__
        
        before_structure = get_structure(before)
        after_structure = get_structure(after)
        
        if before_structure == after_structure:
            return 0.0
        
        # Calculate structural similarity
        similarity = difflib.SequenceMatcher(None, before_structure, after_structure).ratio()
        return 1.0 - similarity

class ValueDeltaAssessor:
    """Assesses value changes between artifacts."""
    
    @staticmethod
    def assess_value_delta(
        before: Dict[str, Any], 
        after: Dict[str, Any],
        semantic_delta: float,
        structural_delta: float
    ) -> Tuple[str, str]:
        """
        Assess value delta and return value change and verdict.
        
        Returns tuple of (value_delta, verdict).
        """
        # Key value indicators
        value_indicators = {
            'ipfs_connected': lambda x: x is True,
            'is_real_ipfs': lambda x: x is True,
            'storage_type': lambda x: x != 'local_dev_store',
            'pinned': lambda x: x is True,
            'verification_proofs': lambda x: x is not None and len(x) > 0,
            'provider_stats': lambda x: x is not None,
            'success_rate': lambda x: isinstance(x, (int, float)) and x > 0.9,
            'confidence_score': lambda x: isinstance(x, (int, float)) and x > 0.7,
        }
        
        before_value = 0
        after_value = 0
        
        for indicator, evaluator in value_indicators.items():
            if indicator in before and evaluator(before[indicator]):
                before_value += 1
            if indicator in after and evaluator(after[indicator]):
                after_value += 1
        
        value_change = after_value - before_value
        
        # Determine value delta
        if value_change > 2:
            value_delta = f"+${value_change * 5}K credibility increase"
        elif value_change > 0:
            value_delta = f"+${value_change * 2}K value increase"
        elif value_change < -2:
            value_delta = f"-${abs(value_change) * 5}K credibility decrease"
        elif value_change < 0:
            value_delta = f"-${abs(value_change) * 2}K value decrease"
        else:
            value_delta = "No significant value change"
        
        # Generate verdict
        if value_change >= 2:
            verdict = "Major upgrade - significantly improved artifact"
        elif value_change >= 1:
            verdict = "Meaningful improvement - enhanced artifact quality"
        elif value_change == 0:
            if semantic_delta > 0.5:
                verdict = "Structural reorganization with similar value"
            else:
                verdict = "Minor changes - minimal impact"
        else:
            verdict = "Degradation - lost value or capabilities"
        
        return value_delta, verdict

class HashDeltaEngine:
    """
    Main engine for artifact transformation appraisal.
    
    Measures how meaning changes while provenance stays machine-verifiable.
    """
    
    def __init__(self):
        self.visual_scorer = VisualHashScorer()
        self.semantic_analyzer = SemanticDeltaAnalyzer()
        self.structural_analyzer = StructuralDeltaAnalyzer()
        self.value_assessor = ValueDeltaAssessor()
    
    def create_artifact(
        self, 
        content: Dict[str, Any], 
        content_type: str,
        source: str,
        storage_refs: Optional[Dict[str, str]] = None
    ) -> Artifact:
        """Create an artifact with canonical hash."""
        content_hash = CanonicalJSON.hash_content(content)
        
        return Artifact(
            content=content,
            content_hash=content_hash,
            content_type=content_type,
            timestamp=datetime.utcnow(),
            source=source,
            storage_refs=storage_refs
        )
    
    def appraise_transformation(
        self,
        before: Artifact,
        after: Artifact
    ) -> HashDelta:
        """
        Appraise the transformation between two artifacts.
        
        Returns comprehensive delta analysis.
        """
        # Calculate various deltas
        hash_visual_similarity = self.visual_scorer.hash_visual_similarity(
            before.content_hash, after.content_hash
        )
        
        text_semantic_delta = self.semantic_analyzer.calculate_semantic_delta(
            before.content, after.content
        )
        
        structural_delta = self.structural_analyzer.calculate_structural_delta(
            before.content, after.content
        )
        
        value_delta, verdict = self.value_assessor.assess_value_delta(
            before.content, after.content, text_semantic_delta, structural_delta
        )
        
        # Determine transformation type
        transformation_type = self._classify_transformation(
            before.content, after.content, text_semantic_delta, structural_delta
        )
        
        # Calculate confidence based on consistency of changes
        confidence = self._calculate_confidence(
            hash_visual_similarity, text_semantic_delta, structural_delta
        )
        
        return HashDelta(
            before_hash=before.content_hash,
            after_hash=after.content_hash,
            hash_visual_similarity=hash_visual_similarity,
            text_semantic_delta=text_semantic_delta,
            structural_delta=structural_delta,
            value_delta=value_delta,
            verdict=verdict,
            transformation_type=transformation_type,
            confidence=confidence
        )
    
    def _classify_transformation(
        self, 
        before: Dict[str, Any], 
        after: Dict[str, Any],
        semantic_delta: float,
        structural_delta: float
    ) -> str:
        """Classify the type of transformation."""
        # Check for common transformation patterns
        patterns = {
            'proof_upgrade': (
                'storage_type' in after and after['storage_type'] != 'local_dev_store' and
                'storage_type' not in before or before.get('storage_type') == 'local_dev_store'
            ),
            'data_enrichment': (
                len(after) > len(before) and
                semantic_delta > 0.3
            ),
            'structural_refactor': (
                structural_delta > 0.5 and
                semantic_delta < 0.3
            ),
            'semantic_shift': (
                semantic_delta > 0.5 and
                structural_delta < 0.3
            ),
            'major_upgrade': (
                semantic_delta > 0.5 and
                structural_delta > 0.5
            )
        }
        
        for pattern_name, condition in patterns.items():
            if condition:
                return pattern_name
        
        return 'minor_change'
    
    def _calculate_confidence(
        self,
        hash_visual_similarity: float,
        semantic_delta: float,
        structural_delta: float
    ) -> float:
        """Calculate confidence in the appraisal."""
        # High confidence when changes are consistent
        if semantic_delta > 0.5 and structural_delta > 0.3:
            return 0.9
        elif semantic_delta > 0.3:
            return 0.7
        elif structural_delta > 0.3:
            return 0.6
        else:
            return 0.4

# Factory function
def get_hash_delta_engine() -> HashDeltaEngine:
    """Get HashDelta engine instance."""
    return HashDeltaEngine()
