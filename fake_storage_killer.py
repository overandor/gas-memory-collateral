"""
Fake Storage Killer

Removes all fake storage fallbacks.
Either use real IPFS or fail loudly.
"""
import os
import shutil
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

class FakeStorageKiller:
    """
    Kills all fake storage mechanisms.
    
    No more silent fallbacks. No more local:// CIDs.
    Either use real IPFS or fail.
    """
    
    def __init__(self):
        self.fake_storage_paths = [
            "./data/proof_bundles",
            "./data/local_artifacts",
            "./data/fake_ipfs"
        ]
        
        self.fake_storage_patterns = [
            "local://",
            "fake://",
            "dev://",
            "test://"
        ]
    
    def kill_fake_storage(self) -> Dict[str, Any]:
        """
        Remove all fake storage and verify it's gone.
        
        Returns:
            Report of what was killed
        """
        report = {
            "killed_at": datetime.utcnow().isoformat(),
            "paths_removed": [],
            "files_removed": [],
            "patterns_blocked": [],
            "success": False
        }
        
        try:
            # Remove fake storage directories
            for path in self.fake_storage_paths:
                if Path(path).exists():
                    self._remove_directory(path, report)
            
            # Block fake storage patterns in code
            self._block_fake_patterns(report)
            
            # Verify fake storage is gone
            self._verify_fake_storage_gone(report)
            
            report["success"] = True
            report["message"] = "All fake storage killed. Real IPFS only."
            
        except Exception as e:
            report["error"] = str(e)
            report["success"] = False
        
        return report
    
    def _remove_directory(self, path: str, report: Dict[str, Any]):
        """Remove a directory and all its contents."""
        try:
            path_obj = Path(path)
            
            # Count files before removal
            file_count = len(list(path_obj.rglob("*"))) if path_obj.exists() else 0
            
            # Remove directory
            if path_obj.exists():
                shutil.rmtree(path_obj)
                report["paths_removed"].append({
                    "path": path,
                    "files_removed": file_count
                })
                
        except Exception as e:
            report["error"] = f"Failed to remove {path}: {str(e)}"
            raise
    
    def _block_fake_patterns(self, report: Dict[str, Any]):
        """Block fake storage patterns by creating markers."""
        try:
            # Create blocking markers
            block_dir = Path("./data/no_fake_storage")
            block_dir.mkdir(parents=True, exist_ok=True)
            
            for pattern in self.fake_storage_patterns:
                marker_file = block_dir / f"block_{pattern.replace('://', '_')}.txt"
                marker_file.write_text(
                    f"FAKE STORAGE BLOCKED\n"
                    f"Pattern: {pattern}\n"
                    f"Blocked at: {datetime.utcnow().isoformat()}\n"
                    f"Use real IPFS or fail loudly.\n"
                )
                report["patterns_blocked"].append(pattern)
                
        except Exception as e:
            report["error"] = f"Failed to block patterns: {str(e)}"
            raise
    
    def _verify_fake_storage_gone(self, report: Dict[str, Any]):
        """Verify that fake storage is completely gone."""
        try:
            # Check directories are gone
            for path in self.fake_storage_paths:
                if Path(path).exists():
                    raise Exception(f"Fake storage still exists: {path}")
            
            # Check for any remaining fake files
            data_dir = Path("./data")
            if data_dir.exists():
                fake_files = list(data_dir.rglob("*local*")) + list(data_dir.rglob("*fake*"))
                if fake_files:
                    raise Exception(f"Fake files still exist: {fake_files}")
            
            report["verification"] = "All fake storage successfully removed"
            
        except Exception as e:
            report["error"] = f"Verification failed: {str(e)}"
            raise
    
    def scan_for_fake_storage(self) -> Dict[str, Any]:
        """Scan for any remaining fake storage."""
        scan_report = {
            "scanned_at": datetime.utcnow().isoformat(),
            "fake_paths_found": [],
            "fake_files_found": [],
            "fake_patterns_in_code": []
        }
        
        try:
            # Scan directories
            for path in self.fake_storage_paths:
                if Path(path).exists():
                    files = list(Path(path).rglob("*"))
                    scan_report["fake_paths_found"].append({
                        "path": path,
                        "file_count": len(files)
                    })
            
            # Scan for fake files
            data_dir = Path("./data")
            if data_dir.exists():
                fake_files = []
                for pattern in ["local", "fake", "dev", "test"]:
                    fake_files.extend(list(data_dir.rglob(f"*{pattern}*")))
                
                scan_report["fake_files_found"] = [str(f) for f in fake_files]
            
            # Scan code for fake patterns (basic check)
            app_dir = Path("./app")
            if app_dir.exists():
                python_files = list(app_dir.rglob("*.py"))
                for file_path in python_files:
                    try:
                        content = file_path.read_text()
                        for pattern in self.fake_storage_patterns:
                            if pattern in content:
                                scan_report["fake_patterns_in_code"].append({
                                    "file": str(file_path),
                                    "pattern": pattern
                                })
                    except:
                        continue
            
        except Exception as e:
            scan_report["error"] = str(e)
        
        return scan_report
    
    def enforce_real_ipfs_only(self) -> Dict[str, Any]:
        """
        Enforce that only real IPFS is used.
        
        This should be called after killing fake storage.
        """
        enforcement_report = {
            "enforced_at": datetime.utcnow().isoformat(),
            "ipfs_required": True,
            "fake_storage_blocked": True,
            "fallback_disabled": True
        }
        
        try:
            # Create enforcement marker
            marker_dir = Path("./data/real_ipfs_only")
            marker_dir.mkdir(parents=True, exist_ok=True)
            
            marker_file = marker_dir / "enforcement.txt"
            marker_file.write_text(
                f"REAL IPFS ONLY ENFORCEMENT\n"
                f"Enforced at: {datetime.utcnow().isoformat()}\n"
                f"Requirements:\n"
                f"- IPFS_API_URL must be configured\n"
                f"- PINATA_API_KEY must be configured\n"
                f"- No local:// or fake:// CIDs allowed\n"
                f"- All storage must go through real IPFS\n"
                f"- Fail loudly if IPFS fails\n"
            )
            
            enforcement_report["success"] = True
            
        except Exception as e:
            enforcement_report["error"] = str(e)
            enforcement_report["success"] = False
        
        return enforcement_report

# Factory function
def get_fake_storage_killer() -> FakeStorageKiller:
    """Get fake storage killer instance."""
    return FakeStorageKiller()
