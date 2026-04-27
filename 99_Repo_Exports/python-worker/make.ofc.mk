# OFC golden replay / benchmarking targets.
#
# Requires tooling from the best-practices patch:
#   tools/ofc_validate_capture.py
#   tools/ofc_capture_fill_expected.py
#   tools/ofc_replay.py
#   tools/bench_ofc_build.py
#
# Typical flow:
#   make -f Makefile.ofc ofc-golden-suite OFC_CAPTURE=/tmp/ofc_inputs.ndjson OFC_GOLDEN=/tmp/ofc_golden.ndjson

PY ?= python

OFC_CAPTURE ?= /tmp/ofc_inputs.ndjson
OFC_GOLDEN ?= /tmp/ofc_golden.ndjson

OFC_VALIDATE_STRICT ?= 1
OFC_REPLAY_STRICT ?= 1

OFC_BENCH_WARMUP ?= 200
OFC_BENCH_ITERS ?= 2000
OFC_BENCH_MODE ?= restore_each
OFC_BUDGET_P95_US ?= 350
OFC_BUDGET_P99_US ?= 900

.PHONY: ofc-validate ofc-fill-expected ofc-replay ofc-bench ofc-golden-suite ofc-clean

ofc-validate:
	$(PY) tools/ofc_validate_capture.py --input $(OFC_CAPTURE) $(if $(filter 1,$(OFC_VALIDATE_STRICT)),--strict-runtime,)

ofc-fill-expected:
	$(PY) tools/ofc_capture_fill_expected.py --input $(OFC_CAPTURE) --output $(OFC_GOLDEN)

ofc-replay:
	$(PY) tools/ofc_replay.py --input $(OFC_GOLDEN) $(if $(filter 1,$(OFC_REPLAY_STRICT)),--strict,)

ofc-bench:
	$(PY) tools/bench_ofc_build.py --input $(OFC_GOLDEN) --warmup $(OFC_BENCH_WARMUP) --iters $(OFC_BENCH_ITERS) \
	  --mode $(OFC_BENCH_MODE) --budget-p95-us $(OFC_BUDGET_P95_US) --budget-p99-us $(OFC_BUDGET_P99_US)

# End-to-end: validate capture -> fill expected -> strict replay -> bench.
ofc-golden-suite: ofc-validate ofc-fill-expected ofc-replay ofc-bench

ofc-clean:
	rm -f $(OFC_GOLDEN)

