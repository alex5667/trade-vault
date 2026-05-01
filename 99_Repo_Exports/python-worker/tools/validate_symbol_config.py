import sys
import os

# Add parent dir to path to allow importing core
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.symbols_config import SymbolsConfig

def main():
    config_path = os.path.join(os.path.dirname(__file__), "../../config/symbols.yml")
    config = SymbolsConfig(config_path=config_path)
    
    try:
        config.load()
        print("Symbols configuration is valid.")
        sys.exit(0)
    except Exception as e:
        print(f"Error validating symbols configuration: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
