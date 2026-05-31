#!/bin/bash
set -e

# Sync direct folders from python-worker to reference...
# Folders: confidence_calculation ml_analysis orderflow_services services utilities ok_rate_logic utils binance_execution
echo "Syncing major modules from python-worker to reference..."
for folder in confidence_calculation ml_analysis orderflow_services services utilities ok_rate_logic utils binance_execution; do
    if [ -d "python-worker/$folder" ]; then
        echo "Updating reference/$folder..."
        mkdir -p "reference/$folder"
        # Use rsync to mirror the folder efficiently
        rsync -a --delete --exclude="__pycache__" --exclude="*.pyc" "python-worker/$folder/" "reference/$folder/"
    else
        echo "Warning: python-worker/$folder does not exist!"
    fi
done

# Sync tick_flow_full from python-worker source
echo "Updating reference/tick_flow_full from python-worker source..."
if [ -d "python-worker/tick_flow_full" ]; then
    mkdir -p "reference/tick_flow_full"
    rsync -a --delete --exclude="__pycache__" --exclude="*.pyc" "python-worker/tick_flow_full/" "reference/tick_flow_full/"
else
    echo "Warning: python-worker/tick_flow_full does not exist!"
fi

# Multi-location collection for liquidation_map
echo "Gathering liquidation_map files from across the project..."
mkdir -p reference/liquidation_map
find python-worker tick_flow_full -type f \( -iname "*liqmap*" -o -iname "*liquidation_map*" \) \
    ! -path "*/__pycache__/*" ! -name "*.pyc" ! -name "liqmap_files.txt" > liqmap_files.txt

while read -r file; do
    cp "$file" "reference/liquidation_map/"
done < liqmap_files.txt
rm liqmap_files.txt

# Multi-location collection for binance_orders (execution related)
echo "Gathering binance_orders (execution related) files..."
mkdir -p reference/binance_orders
# Find relevant files in python-worker/ and subfolders
find python-worker -maxdepth 3 -type f \( -iname "*binance*" -o -iname "*execution*" -o -iname "*trailing*" -o -iname "*order*" \) \
    ! -path "*/__pycache__/*" ! -name "*.pyc" ! -name "binance_orders_files.txt" > binance_orders_files.txt

while read -r file; do
    cp "$file" "reference/binance_orders/"
done < binance_orders_files.txt
rm binance_orders_files.txt

echo "Updating reference update script completed successfully."
