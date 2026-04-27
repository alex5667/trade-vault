#!/usr/bin/env python3
"""
Real-time EV Gate Monitor

Мониторит логи crypto-orderflow сервиса в реальном времени и
отображает EV gate активность с красивым форматированием.

Usage:
    python tools/monitor_ev_gate.py
    python tools/monitor_ev_gate.py --container scanner-crypto-orderflow-2
"""

import subprocess
import sys
import re
from datetime import datetime
import argparse


# ANSI color codes
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def parse_ev_veto_log(line: str) -> dict:
    """
    Парсит лог EV-gate veto:
    
    EV-gate veto: breakout long | BTCUSDT | p=0.58(min=0.55,n=45,src=ema) 
    tp1=50.0bps stop=30.0bps EV=17.4bps < thr=24.0bps
    """
    result = {}
    
    # Extract kind and side
    m = re.search(r'EV-gate veto: (\w+) (\w+)', line)
    if m:
        result['kind'] = m.group(1)
        result['side'] = m.group(2)
    
    # Extract symbol
    m = re.search(r'\| (\w+) \|', line)
    if m:
        result['symbol'] = m.group(1)
    
    # Extract probability
    m = re.search(r'p=([\d.]+)\(min=([\d.]+),n=(\d+),src=(\w+)\)', line)
    if m:
        result['p'] = float(m.group(1))
        result['p_min'] = float(m.group(2))
        result['n'] = int(m.group(3))
        result['src'] = m.group(4)
    
    # Extract bps values
    m = re.search(r'tp1=([\d.]+)bps', line)
    if m:
        result['tp1_bps'] = float(m.group(1))
    
    m = re.search(r'stop=([\d.]+)bps', line)
    if m:
        result['stop_bps'] = float(m.group(1))
    
    m = re.search(r'EV=([\d.]+)bps', line)
    if m:
        result['ev_bps'] = float(m.group(1))
    
    m = re.search(r'thr=([\d.]+)bps', line)
    if m:
        result['thr_bps'] = float(m.group(1))
    
    return result


def format_ev_veto(data: dict) -> str:
    """Форматирует EV veto для красивого вывода."""
    lines = []
    
    # Header
    kind_color = Colors.FAIL if data.get('kind') == 'breakout' else Colors.WARNING
    lines.append(f"{Colors.BOLD}{kind_color}🚫 EV VETO{Colors.ENDC}")
    
    # Signal info
    lines.append(f"   Signal: {data.get('kind', '?')} {data.get('side', '?').upper()} on {data.get('symbol', '?')}")
    
    # Probability
    p = data.get('p', 0)
    p_min = data.get('p_min', 0)
    n = data.get('n', 0)
    src = data.get('src', '?')
    
    p_color = Colors.OKGREEN if p >= p_min else Colors.FAIL
    lines.append(f"   P(TP1): {p_color}{p:.3f}{Colors.ENDC} (min={p_min:.3f}, n={n}, src={src})")
    
    # EV calculation
    tp1_bps = data.get('tp1_bps', 0)
    stop_bps = data.get('stop_bps', 0)
    ev_bps = data.get('ev_bps', 0)
    thr_bps = data.get('thr_bps', 0)
    
    lines.append(f"   Levels: TP1={tp1_bps:.1f}bps, SL={stop_bps:.1f}bps")
    
    ev_color = Colors.FAIL if ev_bps < thr_bps else Colors.OKGREEN
    lines.append(f"   EV:     {ev_color}{ev_bps:.1f}bps{Colors.ENDC} < threshold={thr_bps:.1f}bps")
    
    # Calculation breakdown
    calc_ev = p * tp1_bps - (1 - p) * stop_bps
    lines.append(f"   Formula: {p:.2f} × {tp1_bps:.1f} - {1-p:.2f} × {stop_bps:.1f} = {calc_ev:.1f}bps")
    
    return "\n".join(lines)


def parse_cost_edge_veto(line: str) -> dict:
    """
    Парсит legacy cost-edge veto:
    
    Cost-edge veto: breakout long | BTCUSDT | move=45.0bps < thr=24.0bps
    """
    result = {}
    
    m = re.search(r'Cost-edge veto: (\w+) (\w+)', line)
    if m:
        result['kind'] = m.group(1)
        result['side'] = m.group(2)
    
    m = re.search(r'\| (\w+) \|', line)
    if m:
        result['symbol'] = m.group(1)
    
    m = re.search(r'move=([\d.]+)bps', line)
    if m:
        result['move_bps'] = float(m.group(1))
    
    m = re.search(r'thr=([\d.]+)bps', line)
    if m:
        result['thr_bps'] = float(m.group(1))
    
    return result


def monitor_logs(container: str):
    """Мониторит логи контейнера."""
    
    cmd = ["docker", "logs", "-f", container]
    
    print(f"{Colors.HEADER}{Colors.BOLD}=== EV Gate Real-time Monitor ==={Colors.ENDC}")
    print(f"Container: {container}")
    print("Watching for: EV-gate veto, Cost-edge veto")
    print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}\n")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        for line in process.stdout:
            line = line.strip()
            
            # EV-gate veto
            if "EV-gate veto" in line:
                data = parse_ev_veto_log(line)
                if data:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    print(f"\n[{timestamp}]")
                    print(format_ev_veto(data))
                    print()
            
            # Legacy cost-edge veto
            elif "Cost-edge veto" in line and "EV-gate" not in line:
                data = parse_cost_edge_veto(line)
                if data:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    print(f"\n[{timestamp}] {Colors.WARNING}⚠️  Legacy Cost-Edge Veto{Colors.ENDC}")
                    print(f"   {data.get('kind', '?')} {data.get('side', '?')} on {data.get('symbol', '?')}")
                    print(f"   Move: {data.get('move_bps', 0):.1f}bps < {data.get('thr_bps', 0):.1f}bps")
                    print()
            
            # Insufficient stats (warm-up)
            elif "insufficient_stats_fail_open" in line:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] {Colors.OKCYAN}ℹ️  Warm-up: insufficient stats (fail-open){Colors.ENDC}")
    
    except KeyboardInterrupt:
        print(f"\n{Colors.OKGREEN}Monitoring stopped.{Colors.ENDC}")
        process.terminate()
    except Exception as e:
        print(f"{Colors.FAIL}Error: {e}{Colors.ENDC}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Monitor EV gate activity in real-time")
    parser.add_argument("--container", default="scanner-crypto-orderflow", 
                       help="Docker container name")
    
    args = parser.parse_args()
    
    monitor_logs(args.container)


if __name__ == "__main__":
    main()
