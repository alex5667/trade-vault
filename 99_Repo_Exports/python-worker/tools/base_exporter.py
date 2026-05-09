
class PlainTextResponse:
    def __init__(self, content: str, media_type: str = "text/plain; version=0.0.4"):
        self.content = content
        self.media_type = media_type

class BaseExporter:
    def __init__(self, port: int = 8000):
        self.port = port

    def get_metrics_response(self) -> PlainTextResponse:
        raise NotImplementedError("Subclasses must implement get_metrics_response")

    def run(self):
        try:
            from http.server import BaseHTTPRequestHandler, HTTPServer

            exporter_instance = self

            class MetricsHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path == '/metrics':
                        try:
                            response = exporter_instance.get_metrics_response()
                            self.send_response(200)
                            self.send_header('Content-type', response.media_type)
                            self.end_headers()
                            self.wfile.write(response.content.encode('utf-8'))
                        except Exception as e:
                            self.send_response(500)
                            self.end_headers()
                            self.wfile.write(f"Error: {e}".encode())
                    else:
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(b"Not Found")

            server = HTTPServer(('0.0.0.0', self.port), MetricsHandler)
            print(f"Starting exporter on port {self.port}...")
            server.serve_forever()
        except ImportError:
            print("http.server not available, exporter cannot run")
