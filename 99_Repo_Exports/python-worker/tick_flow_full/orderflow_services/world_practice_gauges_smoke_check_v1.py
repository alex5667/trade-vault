# -*- coding: utf-8 -*-
"""Compat wrapper for world-practice smoke-check.

Kept under tick_flow_full/ for deployments that reference this subtree.
"""

from __future__ import annotations

from orderflow_services.world_practice_gauges_smoke_check_v1 import main


if __name__ == "__main__":
    raise SystemExit(main())
