import urllib.request

try:
    resp = urllib.request.urlopen("http://localhost:9830")
    for line in resp.read().decode('utf-8').split("\n"):
        if "worker_lag_ms" in line:
            print(line)
except Exception as e:
    print(f"Error: {e}")
