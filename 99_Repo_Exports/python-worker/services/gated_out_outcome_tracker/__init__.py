"""Gated-out signal outcome tracker.

Consumes confidence-gated-out signals from stream:signals:gated_out, waits
for a fixed horizon, queries the tick stream for the realised price path,
and writes a synthetic outcome (return_bps, r_mult, tp/sl hit, y) to
stream:signals:gated_out_outcomes. Stage-1 of «is the confidence gate
worth keeping?» investigation.
"""
