import sys
try:
    from paths import setup_path
except ImportError:
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent))
    from paths import setup_path

setup_path()

def verify_imports():
    print("Verifying imports...")
    try:
        # Check Models
        from supply_chain.models.gnn import SupplyChainGNN
        print("✅ GNN Model import successful")
        
        from supply_chain.models.mlp import SupplyChainNet
        print("✅ MLP Model import successful")
        
        # Check Logic
        from supply_chain.models.inference import get_current_graph_snapshot
        print("✅ Inference module import successful")
        
        # Check App Components
        from supply_chain.app.components.map import render_pydeck_map
        print("✅ Map Component import successful")
        
        # Check Main App (might trigger streamlit warnings but imports should work)
        # Main app has code at module level that runs, so just importing it might run it?
        # No, because the main block is not protected by if __name__ == "__main__" in main.py?
        # Actually proper streamlit apps just run top to bottom.
        # But importing it will execute the top-level code.
        # The top level code calls st.set_page_config which errors if not run via streamlit.
        # So we skip importing main.py directly for this test, relying on component checks.
        
        print("All critical imports verified!")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"⚠️ Runtime error during import (expected for Streamlit app): {e}")

if __name__ == "__main__":
    verify_imports()
