import urllib.request
data = b'of_prom_rules_bundle_last_ok 1\n'
req = urllib.request.Request(
    'http://localhost:9091/metrics/job/local_health',
    data=data,
    method='POST'
)
try:
    urllib.request.urlopen(req)
    print("Pushed OK")
except Exception as e:
    print("Push Failed:", e)
