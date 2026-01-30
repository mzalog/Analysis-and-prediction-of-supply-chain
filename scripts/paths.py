import sys
import os
from pathlib import Path

def setup_path():
    """
    Adds project root/src to sys.path and returns the project root Path object.
    Assumes this script is located in [PROJECT_ROOT]/scripts/
    """
    # scripts_dir = [root]/scripts
    scripts_dir = Path(__file__).resolve().parent
    # project_root = [root]
    project_root = scripts_dir.parent
    
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.append(str(src_path))
        
    return project_root
