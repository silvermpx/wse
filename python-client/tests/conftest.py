import sys
from pathlib import Path

# Ensure wse_client is importable without install
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
