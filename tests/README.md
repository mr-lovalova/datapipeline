# Tests layout

- `unit/` – fast, isolated tests by area:
  - `analysis/`, `build/`, `cli/`, `config/`, `execution/`, `integrations/`, `io/`, `operations/`, `parsers/`, `pipeline/`, `profiles/`, `services/`, `sources/`, `transforms/`, `utils/`
- `integration/` – fixture-backed end-to-end flows; use `copy_fixture` to work on a temp copy.
- `fixtures/` – dataset fixtures for integration tests; never mutate directly in a test (always go through `copy_fixture` from `tests/conftest.py`).
