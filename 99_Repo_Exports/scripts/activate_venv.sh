#!/bin/bash
# Activate virtual environment for XAUUSD services

cd "$(dirname "$0")/python-worker"

if [ -d "venv" ]; then
    echo "✅ Activating virtual environment..."
    source venv/bin/activate
    echo "🐍 Python: $(python --version)"
    echo "📍 Location: $(which python)"
    echo ""
    echo "Virtual environment activated!"
    echo ""
    echo "Now you can run:"
    echo "  python -m services.book_analytics_service"
    echo "  python -m services.notify_receiver"
    echo "  ./start_obi_services.sh"
    echo ""
    exec bash
else
    echo "❌ Virtual environment not found!"
    echo "Create it with: python3 -m venv venv"
    exit 1
fi
