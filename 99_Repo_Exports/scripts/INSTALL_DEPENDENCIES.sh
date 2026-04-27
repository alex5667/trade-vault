#!/bin/bash
# Install Dependencies for OBI Events & PNG Rendering

echo "📦 Installing dependencies for XAUUSD Order Flow v7.1.1"
echo ""

# Option 1: System packages (recommended for Ubuntu/Debian)
echo "Option 1: System packages (apt)"
echo "  sudo apt update"
echo "  sudo apt install -y python3-aiohttp python3-matplotlib python3-fastapi python3-uvicorn"
echo ""

# Option 2: Virtual environment
echo "Option 2: Virtual environment (recommended for development)"
echo "  python3 -m venv venv"
echo "  source venv/bin/activate"
echo "  pip install aiohttp matplotlib fastapi uvicorn"
echo ""

# Option 3: User install
echo "Option 3: User install (pip --user)"
echo "  pip install --user aiohttp matplotlib"
echo ""

# Option 4: Break system packages (not recommended)
echo "Option 4: Override system protection (not recommended)"
echo "  pip install --break-system-packages aiohttp matplotlib"
echo ""

echo "════════════════════════════════════════════════════════"
echo "Choose your option and run the corresponding commands."
echo ""

# Check current environment
echo "Current Python environment:"
python3 --version
echo ""

echo "Installed packages (user):"
pip list --user 2>/dev/null | grep -E "(aiohttp|matplotlib|fastapi|uvicorn)" || echo "  None found in user packages"
echo ""

echo "System packages:"
dpkg -l | grep -E "python3-(aiohttp|matplotlib|fastapi|uvicorn)" || echo "  None found in system packages"
echo ""

echo "════════════════════════════════════════════════════════"
echo ""
echo "Recommended: Option 1 (system packages)"
echo ""
echo "Run:"
echo "  sudo apt update && sudo apt install -y python3-aiohttp python3-matplotlib"
echo ""
