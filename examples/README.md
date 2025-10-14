Example projects and scripts that show how to use the ML adapters in a simple, convention-first way.

Suggested workflow:

- Create or activate a virtual environment
- Install the package (and ML extras) in editable mode:
  - `python -m pip install -U pip setuptools wheel`
  - `python -m pip install -e '.[ml]'`
- Run one of the example scripts from the repo root, e.g.:
  - `python examples/minimal_project/run_dataframe.py`
  - `python examples/minimal_project/run_torch.py`

Notes:
- The example project keeps configs under `config/` inside the example folder, mirroring the recommended layout for end users.
- No vector-level transforms are configured in the minimal recipe, so there’s no need to pre-generate a partitions manifest.
