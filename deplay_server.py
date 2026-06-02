# save as delay_server.py
from http.server import HTTPServer, BaseHTTPRequestHandler
import time

class DelayHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if '/delay/' in self.path:
            try:
                delay = int(self.path.split('/')[-1])
                print(f"Waiting {delay} seconds...")
                time.sleep(delay)
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"Delayed {delay} seconds".encode())
            except:
                self.send_response(400)
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
    
    def log_message(self, format, *args):
        print(f"{time.strftime('%H:%M:%S')} - {args}")

if __name__ == '__main__':
    port = 8081
    server = HTTPServer(('0.0.0.0', port), DelayHandler)
    print(f"Delay server running on port {port}")
    print(f"Test with: curl http://localhost:{port}/delay/20")
    server.serve_forever()