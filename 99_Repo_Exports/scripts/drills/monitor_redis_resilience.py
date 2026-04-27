#!/usr/bin/env python3
import redis
import time
import os
import sys
from datetime import datetime

# Настройки из окружения
REDIS_HOST = os.getenv("REDIS_HOST", "redis-worker-1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CRITICAL_STREAMS = ["stream:ticks", "stream:book_1000FLOKIUSDT", "events:decision_snapshot"]

class DrillMonitor:
    def __init__(self):
        self.client = None
        self.last_status = True

    def connect(self):
        try:
            self.client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1
            )
            self.client.ping()
            return True
        except Exception:
            return False

    def check_streams(self):
        results = {}
        for s in CRITICAL_STREAMS:
            try:
                info = self.client.xinfo_stream(s)
                results[s] = {
                    "len": info.get("length"),
                    "last_id": info.get("last-generated-id"),
                    "status": "OK"
                }
            except Exception as e:
                results[s] = {"status": f"ERROR: {str(e)}"}
        return results

    def run(self):
        print(f"🚀 DRILL MONITOR STARTING targets: {REDIS_HOST}:{REDIS_PORT}")
        print("="*60)
        
        while True:
            now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            is_connected = self.connect()
            
            if is_connected:
                if not self.last_status:
                    print(f"[{now}] ✅ REDIS RECOVERED")
                
                streams = self.check_streams()
                stream_info = " | ".join([f"{k}: {v['status']}" for k, v in streams.items()])
                print(f"[{now}] 🟢 ONLINE | {stream_info}")
                self.last_status = True
            else:
                if self.last_status:
                    print(f"[{now}] 🚨 REDIS DOWN / CONNECTION LOST")
                print(f"[{now}] 🔴 OFFLINE")
                self.last_status = False
            
            time.sleep(1)

if __name__ == "__main__":
    try:
        DrillMonitor().run()
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped")
