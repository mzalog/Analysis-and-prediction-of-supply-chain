
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))

# Set python path to include src to allow imports
sys.path.append(str(project_root / "src"))

from supply_chain.research.experiments import run_experiment

try:
    print("Running verification experiment...")
    report = run_experiment(
        experiment_name="verification_test",
        num_trucks=2,  # Small number for speed
        num_orders=10, # Small number for speed
        epochs=2       # Few epochs for speed
    )
    print("Verification Successful!")
    print(report)
except Exception as e:
    print(f"Verification Failed: {e}")
    import traceback
    traceback.print_exc()

# Also verify that torch is importable and functional
import torch
print(f"Torch version: {torch.__version__}")
print(f"CUDA available: {torch.cuda.is_available()}")
