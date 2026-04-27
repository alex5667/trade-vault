# ExecHealth ACL Drift Runbook (P12)

This runbook covers alerts raised by the `exec_health_freeze_acl_drift_exporter_v1`.

## 🚨 OF_ExecHealth_FreezeACLDrift_Crit
**What it means:** The active ACL rules in Redis do not match the expected deployment contract defined in `exec_health_freeze_acl_contract.py`.

**Why it matters:** Someone or something may have manually altered ACLs (e.g., granting a user broader permissions than allowed) or the deployment is out of sync after a code update.

**How to verify:**
1. Check the Grafana dashboard: **ExecHealth Freeze ACL Drift (v1)**. It will show which user has the drift.
2. Run the policy check script to see the exact violation:
   ```bash
   python orderflow_services/exec_health_freeze_acl_policy_v1.py check
   ```

**How to fix:**
Re-apply the deployment contract.
```bash
python orderflow_services/exec_health_freeze_acl_policy_v1.py apply --reload-check
```

---

## 🚨 OF_ExecHealth_FreezeDefaultUserConnected_Crit
**What it means:** There are active Redis connections logged in as the `default` user.

**Why it matters:** P11/P12 requires the default user to be disabled (`off`). If it is enabled and apps are using it, they bypass the fine-grained surface rules. All ExecHealth apps must use `AUTH exec_health_freeze_reader/writer <pass>`.

**How to verify:**
1. Check the Grafana dashboard for the "Invalid Connections" panel.
2. Manually list clients:
   ```bash
   redis-cli CLIENT LIST | grep user=default
   ```

**How to fix:**
1. Identify the leaking application via the IP/port in `CLIENT LIST`.
2. Update the application's configuration to use the correct `REDIS_URL` with username and password.
3. Restart the application.
4. Run `python orderflow_services/exec_health_freeze_acl_policy_v1.py apply` to forcefully disable `default`.

---

## ⚠️ OF_ExecHealth_FreezeACLFileMissing_Warn
**What it means:** Redis is not configured with an `aclfile` directive in `redis.conf`.

**Why it matters:** The `ACL SAVE` command cannot write to disk. If Redis restarts, all ACL users and rules will be lost and default back to `nopass`.

**How to verify:**
```bash
redis-cli CONFIG GET aclfile
```
If it returns an empty string, it's unconfigured.

**How to fix:**
1. Create an empty file (e.g., `touch /data/users.acl`).
2. Update `redis.conf` to include `aclfile /data/users.acl`.
3. Restart Redis or inject it live:
   ```bash
   redis-cli CONFIG SET aclfile /data/users.acl
   ```
4. Run `ACL SAVE`.

---

## ⚠️ OF_ExecHealth_FreezeACLDriftExporter_Stale_Warn
**What it means:** The exporter loop crashed or hung. We are currently blind to ACL violations.

**Why it matters:** Security controls cannot be verified if the observer is broken.

**How to fix:**
Check exporter logs:
```bash
docker logs crypto-orderflow-exec-health-acl-drift-exporter
```
Restart the service if necessary.
