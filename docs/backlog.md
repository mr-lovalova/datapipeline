# Backlog

## Cache V2 (Checkpoint-Based)

Current status:
- Session fan-out cache was removed from core pipeline to keep orchestration simple and explicit.
- `build_feature_pipeline`/`build_vector_pipeline` now use the direct non-cached flow.

Proposed redesign:
1. Add explicit stage checkpoints (not hidden source streams).
2. Keep cache/checkpoint concerns outside pipeline orchestration.
3. Define manifest for each checkpoint:
- stage boundary
- config/signature hash
- upstream dependency hashes
- row count + path
4. Resume/start options should select a valid checkpoint and continue downstream stages only.
5. Visuals should render checkpoints via existing source-like progress adapters.
6. Durable checkpoints (artifacts/runs) and ephemeral caches should share the same model but different retention policy.
