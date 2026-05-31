import os
import sys
import time
import json
import logging
import redis
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("research_guard_exporter")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
METRICS_KEY = os.getenv("STRATEGY_RESEARCH_GUARD_METRICS_KEY", "metrics:strategy_research_guard:last")
BLOCKER_KEY = os.getenv("STRATEGY_RESEARCH_GUARD_BLOCKER_KEY", "cfg:research_guard:blocker:v1")
PORT = int(os.getenv("PROMETHEUS_PORT", "9140"))

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; version=0.0.4')
            self.end_headers()
            
            try:
                r = redis.from_url(REDIS_URL)
                metrics_data = r.get(METRICS_KEY)
                blocker_data = r.get(BLOCKER_KEY)
                
                output = []
                output.append("# HELP strategy_guard_up Exporter is running")
                output.append("# TYPE strategy_guard_up gauge")
                output.append("strategy_guard_up 1")
                
                if metrics_data:
                    metrics = json.loads(metrics_data)
                    for k, v in metrics.items():
                        if isinstance(v, (int, float, bool)):
                            val = 1 if v is True else (0 if v is False else v)
                            output.append(f"# TYPE strategy_guard_{k} gauge")
                            output.append(f"strategy_guard_{k} {val}")
                            
                if blocker_data:
                    blocker = json.loads(blocker_data)
                    output.append("# TYPE strategy_guard_blocker_active gauge")
                    output.append(f"strategy_guard_blocker_active {1 if blocker.get('blocker_active') else 0}")
                    
                    output.append("# TYPE strategy_guard_report_only gauge")
                    output.append(f"strategy_guard_report_only {int(blocker.get('report_only', 1))}")
                
            except Exception as e:
                logger.error(f"Error fetching metrics: {e}")
                output = [
                    "# HELP strategy_guard_up Exporter logic error",
                    "# TYPE strategy_guard_up gauge",
                    "strategy_guard_up 0"
                ]
                
            self.wfile.write(("\n".join(output) + "\n").encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def main():
    server = HTTPServer(('0.0.0.0', PORT), MetricsHandler)
    logger.info(f"Starting Strategy Research Guard Exporter on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    main()
