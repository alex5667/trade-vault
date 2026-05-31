#!/bin/bash
# Fix Docker DNS resolution timeout issues
# This script configures Docker daemon to use reliable DNS servers

set -e

echo "🔧 Fixing Docker DNS resolution..."
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  This script needs sudo privileges to configure Docker daemon."
    echo "   Please run: sudo bash fix_docker_dns.sh"
    exit 1
fi

# Create /etc/docker directory if it doesn't exist
mkdir -p /etc/docker

# Backup existing daemon.json if it exists
if [ -f /etc/docker/daemon.json ]; then
    echo "📋 Backing up existing /etc/docker/daemon.json..."
    cp /etc/docker/daemon.json /etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)
fi

# Create or update daemon.json with DNS configuration
echo "📝 Configuring Docker daemon DNS..."
cat > /etc/docker/daemon.json << 'EOF'
{
  "dns": ["8.8.8.8", "8.8.4.4", "1.1.1.1"]
}
EOF

echo "✅ Docker daemon DNS configured successfully!"
echo ""
echo "🔄 Restarting Docker daemon..."
systemctl restart docker

echo ""
echo "✅ Docker daemon restarted. DNS configuration is now active."
echo ""
echo "🧪 Testing DNS resolution..."
if docker run --rm alpine:latest nslookup registry-1.docker.io 2>&1 | grep -q "registry-1.docker.io"; then
    echo "✅ DNS resolution test passed!"
else
    echo "⚠️  DNS resolution test failed. Please check your network connection."
fi

echo ""
echo "📝 You can now run: make up"



