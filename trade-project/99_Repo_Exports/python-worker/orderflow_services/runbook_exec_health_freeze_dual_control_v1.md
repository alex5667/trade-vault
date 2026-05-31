# ExecHealth Freeze Dual Control (v1)

Thaw workflow:
1. `prepare-thaw` by operator A with current ack nonce.
2. `approve-thaw` by operator B (`B != A`).
3. `commit-thaw` by the approved operator B.

A thaw is valid only when a signed `manual_ack_thaw_commit` event exists for the same `request_id` and nonce.
Simple deletion of control/state keys is not a valid thaw path.
