#!/bin/bash
# Quick start script for Signal Generator

echo "═══════════════════════════════════════════"
echo "  🎯 Starting Signal Generator"
echo "═══════════════════════════════════════════"
echo ""

cd "$(dirname "$0")/signal-generator"

# Check if Docker is available
if command -v docker &> /dev/null; then
    echo "✅ Docker detected"
    echo ""
    
    # Build image
    echo "Building Docker image..."
    docker build -t signal-generator . || { echo "❌ Build failed"; exit 1; }
    
    echo ""
    echo "Starting Signal Generator..."
    echo "Press Ctrl+C to stop"
    echo ""
    
    # Run container
    docker run --rm \
        --name signal-generator \
        --network scanner_infra_scanner-network \
        --env-file config.env \
        signal-generator
else
    echo "⚠️  Docker not found, using Python..."
    echo ""
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 not found"
        exit 1
    fi
    
    # Install dependencies if needed
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
    else
        source venv/bin/activate
    fi
    
    # Load config
    export $(cat config.env | xargs)
    
    # Change URLs for localhost
    export GATEWAY_URL="http://127.0.0.1:8090"
    export OBI_SERVICE_URL="http://127.0.0.1:8088"
    
    echo "Starting Signal Generator..."
    python3 signal_generator.py
fi
