
import os
import subprocess

with open('patch_trade_p67_strategy_research_stats_alert_policy_ttl_v1.clean.diff') as f:
    diff = f.read()

# We need to split the diff for python-worker and tick_flow_full
python_worker_diff = []
tick_flow_full_diff = []
current = []
dest = 'python-worker'

for line in diff.splitlines():
    if line.startswith('--- orderflow_services/'):
        current = python_worker_diff
        dest = 'python-worker'
        current.append(line)
    elif line.startswith('+++ orderflow_services/'):
        current.append(line)
    elif line.startswith('--- tick_flow_full/'):
        current = tick_flow_full_diff
        dest = 'tick_flow_full'
        current.append(line.replace('--- tick_flow_full/', '--- '))
    elif line.startswith('+++ tick_flow_full/'):
        current.append(line.replace('+++ tick_flow_full/', '+++ '))
    elif line.startswith('--- /dev/null') or line.startswith('+++ /dev/null'):
        current.append(line)
    else:
        current.append(line)

with open('pw.diff', 'w') as f: f.write(chr(10).join(python_worker_diff))
with open('tf.diff', 'w') as f: f.write(chr(10).join(tick_flow_full_diff))

print('Applying python-worker diff')
os.system('patch -f -p0 -d python-worker < pw.diff')
print('Applying tick_flow_full diff')
os.system('patch -f -p0 -d tick_flow_full < tf.diff')
