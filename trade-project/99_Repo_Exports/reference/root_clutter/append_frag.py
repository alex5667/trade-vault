with open('docker-compose-python-workers.yml', 'r') as f:
    lines = f.read()

with open('/tmp/append.yml', 'r') as f:
    frag = f.read()

idx = lines.rfind('\nnetworks:')
out = lines[:idx] + '\n' + frag + lines[idx:]

with open('docker-compose-python-workers.yml', 'w') as f:
    f.write(out)
