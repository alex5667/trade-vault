#!/bin/bash
# Install Trailing Recommender Timer

set -e

echo "Installing Trailing Recommender systemd service and timer..."

# Copy service and timer files
sudo cp trailing-recommender.service /etc/systemd/system/
sudo cp trailing-recommender.timer /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable and start timer
sudo systemctl enable trailing-recommender.timer
sudo systemctl start trailing-recommender.timer

echo "✅ Trailing Recommender timer installed and started"
echo "Check status with: systemctl status trailing-recommender.timer"
echo "Check logs with: journalctl -u trailing-recommender.service"
