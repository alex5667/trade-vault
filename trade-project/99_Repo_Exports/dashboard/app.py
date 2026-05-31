#!/usr/bin/env python3
"""
ROC/AUC Dashboard - Interactive performance visualization.

Provides web interface for analyzing signal performance using ROC curves
and feature distributions.

Requires:
    - Joined data with features + labels (from export_labels_pnl.py)
    - Config file with thresholds (xauusd.yaml)

Usage:
    python3 -m dashboard.app
    # Then open: http://localhost:8091
"""

import os
import pathlib
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from sklearn.metrics import roc_curve, roc_auc_score
import yaml


# Configuration
DATA_PATH = pathlib.Path(os.getenv(
    "DASHBOARD_DATA",
    "data/labels/joined_pnl.parquet"
))
CFG_PATH = pathlib.Path(os.getenv(
    "DASHBOARD_CONFIG",
    "config/defaults/xauusd.yaml"
))
PORT = int(os.getenv("DASHBOARD_PORT", "8091"))


app = FastAPI(
    title="ROC/AUC Dashboard",
    description="Signal performance visualization",
    version="7.1.0"
)


def load_data() -> pd.DataFrame:
    """Load joined data."""
    if not DATA_PATH.exists():
        return pd.DataFrame()
    
    if DATA_PATH.suffix == ".parquet":
        return pd.read_parquet(DATA_PATH)
    return pd.read_csv(DATA_PATH)


def load_config() -> Dict:
    """Load configuration."""
    if not CFG_PATH.exists():
        return {}
    
    with open(CFG_PATH) as f:
        return yaml.safe_load(f)


@app.get("/", response_class=HTMLResponse)
def index():
    """Main dashboard page."""
    df = load_data()
    
    if df.empty:
        return """
        <html>
        <head><meta charset="utf-8"><title>ROC Dashboard</title></head>
        <body>
            <h2>⚠️ No Data Available</h2>
            <p>Please export joined data first:</p>
            <pre>python3 services/export_labels_pnl.py ...</pre>
        </body>
        </html>
        """
    
    # Prepare features
    if "weakProgress" in df.columns:
        df["weakProgress_inv"] = df["weakProgress"].astype(float)
    
    # Label (from pnl or profit)
    if "pnl" in df.columns:
        label = (df["pnl"] > 0).astype(int)
    elif "profit" in df.columns:
        label = (df["profit"] > 0).astype(int)
    else:
        return """
        <html>
        <head><meta charset="utf-8"></head>
        <body><h2>❌ No PnL/profit column found</h2></body>
        </html>
        """
    
    # Create ROC curves
    figs = []
    
    features_to_plot = [
        ("Delta Z-score", "delta_z"),
        ("OBI Signed", "obi_signed") if "obi_signed" in df.columns else ("OBI", "obi"),
        ("Weak Progress", "weakProgress_inv") if "weakProgress_inv" in df.columns else None
    ]
    
    for item in features_to_plot:
        if item is None:
            continue
        
        name, col = item
        
        if col not in df.columns:
            continue
        
        s = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        
        try:
            fpr, tpr, _ = roc_curve(label, s)
            auc = roc_auc_score(label, s)
            
            figs.append(
                go.Scatter(
                    x=fpr,
                    y=tpr,
                    mode="lines",
                    name=f"{name} (AUC={auc:.3f})"
                )
            )
        except Exception as e:
            print(f"Failed to compute ROC for {name}: {e}")
    
    # Add diagonal
    figs.append(
        go.Scatter(
            x=[0, 1],
            y=[0, 1],
            mode="lines",
            line=dict(dash="dash", color="gray"),
            name="Random",
            showlegend=False
        )
    )
    
    layout = go.Layout(
        title="ROC Curves - Signal Performance",
        xaxis=dict(title="False Positive Rate"),
        yaxis=dict(title="True Positive Rate"),
        width=1000,
        height=700,
        hovermode="closest"
    )
    
    roc_html = go.Figure(data=figs, layout=layout).to_html(
        full_html=False,
        include_plotlyjs='cdn'
    )
    
    # Load current thresholds
    cfg = load_config()
    thresholds_html = ""
    
    if cfg:
        thresholds_html = "<h3>📊 Current Thresholds</h3><pre>" + yaml.dump(cfg, sort_keys=False) + "</pre>"
    
    # Statistics
    total_trades = len(df)
    if "pnl" in df.columns:
        total_pnl = df["pnl"].sum()
        avg_pnl = df["pnl"].mean()
        win_rate = (df["pnl"] > 0).mean()
    elif "profit" in df.columns:
        total_pnl = df["profit"].sum()
        avg_pnl = df["profit"].mean()
        win_rate = (df["profit"] > 0).mean()
    else:
        total_pnl = avg_pnl = win_rate = 0
    
    stats_html = f"""
    <h3>📈 Overall Statistics</h3>
    <table style="border-collapse: collapse;">
        <tr><td style="padding: 5px;"><b>Total Trades:</b></td><td style="padding: 5px;">{total_trades}</td></tr>
        <tr><td style="padding: 5px;"><b>Total P&L:</b></td><td style="padding: 5px;">${total_pnl:.2f}</td></tr>
        <tr><td style="padding: 5px;"><b>Average P&L:</b></td><td style="padding: 5px;">${avg_pnl:.2f}</td></tr>
        <tr><td style="padding: 5px;"><b>Win Rate:</b></td><td style="padding: 5px;">{win_rate:.1%}</td></tr>
    </table>
    """
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>XAUUSD ROC/AUC Dashboard</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h2 {{ color: #333; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            pre {{ background: #f4f4f4; padding: 10px; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>🚀 XAUUSD Order Flow - ROC/AUC Dashboard v7.1</h2>
            
            {stats_html}
            
            {roc_html}
            
            {thresholds_html}
            
            <hr>
            <p><small>Data: {DATA_PATH} | Config: {CFG_PATH}</small></p>
        </div>
    </body>
    </html>
    """


@app.get("/api/stats")
def get_stats():
    """Get statistics as JSON."""
    df = load_data()
    
    if df.empty:
        raise HTTPException(404, "No data")
    
    total_trades = len(df)
    
    if "pnl" in df.columns:
        total_pnl = float(df["pnl"].sum())
        avg_pnl = float(df["pnl"].mean())
        win_rate = float((df["pnl"] > 0).mean())
    elif "profit" in df.columns:
        total_pnl = float(df["profit"].sum())
        avg_pnl = float(df["profit"].mean())
        win_rate = float((df["profit"] > 0).mean())
    else:
        total_pnl = avg_pnl = win_rate = 0.0
    
    return {
        "total_trades": total_trades,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "win_rate": win_rate
    }


@app.get("/healthz")
def health():
    """Health check."""
    return {"ok": True, "data_exists": DATA_PATH.exists()}


if __name__ == "__main__":
    import uvicorn
    print(f"🎨 ROC/AUC Dashboard starting on port {PORT}...")
    print(f"   Data: {DATA_PATH}")
    print(f"   Config: {CFG_PATH}")
    print(f"   URL: http://localhost:{PORT}")
    print()
    
    uvicorn.run(
        "dashboard.app:app",
        host="127.0.0.1",
        port=PORT,
        reload=False
    )

