# Backlog

## Additional Checkpoints

Current status:
- Hidden session fan-out caching was removed from pipeline orchestration.
- `series` is now an explicit durable artifact with a manifest, compressed
  feature/target shards, file fingerprints, config freshness, and graph dependencies.
- Sample assembly requires that artifact instead of silently switching between
  cached and uncached execution paths.

Possible future work:

1. Add other explicit stage checkpoints only where recomputation cost justifies
   another public artifact contract.
2. Support content-addressed source versions for metadata-preserving local
   changes and remote or opaque inputs.
3. Let resume options select a validated checkpoint without adding hidden source
   substitution to pipeline construction.
4. Give durable artifacts and ephemeral spill/cache data one explicit retention
   policy model while keeping their lifetimes distinct.
