PYTHON ?= python3
RUFF ?= ruff

.PHONY: check lint test e2e

check: lint test

lint:
	$(RUFF) check .

test:
	$(PYTHON) -m pytest

# Round-trip against a real Orbit (see README "Round-trip against a local Orbit").
e2e:
	$(PYTHON) scripts/e2e_roundtrip.py
