# Pipeline Architecture (WIP)

```text
raw source ──▶ loader/parser DTOs ──▶ canonical stream ──▶ record policies
      └──▶ feature wrapping ──▶ stream regularization ──▶ feature transforms/sequence
      └──▶ vector assembly ──▶ postprocess transforms
```

See "Preview Indices (serve --preview-index)" for the detailed preview-index breakdown.

#### Visual Flowchart

```mermaid
flowchart TB
  subgraph CLI & Project config
    cliInflow[jerry inflow create]
    cliSource[jerry source create]
    cliDomain[jerry domain create]
    cliStream[jerry stream]
    cliServe[jerry serve]
    project[[project.yaml]]
    sourcesCfg[sources/*.yaml]
    streamsCfg[streams/*.yaml]
    datasetCfg[dataset.yaml]
    postprocessCfg[postprocess.yaml]
  end

  cliInflow --> sourcesCfg
  cliInflow --> streamsCfg
  cliSource --> sourcesCfg
  cliDomain --> domainPkg
  cliStream --> streamsCfg
  cliServe --> vectorSamples
  project -.->|paths.sources| sourcesCfg
  project -.->|paths.streams| streamsCfg
  project -.->|paths.dataset| datasetCfg
  project -.->|paths.postprocess| postprocessCfg

  subgraph Plugin code
    domainPkg[domains/*]
    mappersPkg[mappers/*]
  end

  cliStream --> mappersPkg
  domainPkg -. domain models .-> mappersPkg

  subgraph Registries
    registrySources[sources]
    registryStreamSources[stream_sources]
    registryMappers[mappers]
    registryRecordOps[record_ops]
    registryStreamOps[stream_ops]
    registryDebugOps[debug_ops]
  end

  subgraph Source wiring
    rawData[(external data)]
    transportSpec[transport + format]
    loaderEP[loader ep]
    parserEP[parser ep]
    sourceArgs[loader args]
    sourceNode[Source]
    dtoStream[(DTOs)]
  end

  sourcesCfg --> transportSpec
  sourcesCfg --> loaderEP
  sourcesCfg --> parserEP
  sourcesCfg --> sourceArgs
  transportSpec -. select fs/http/synth .-> loaderEP
  loaderEP -. build loader .-> sourceNode
  parserEP -. build parser .-> sourceNode
  sourceArgs -. paths/creds .-> sourceNode
  rawData --> sourceNode --> dtoStream
  sourcesCfg -. build_source_from_spec .-> registrySources
  streamsCfg -. stream_id + source .-> registryStreamSources
  registrySources -. alias -> Source .-> registryStreamSources

  subgraph Canonical stream
    mapperEP[mapper ep]
    recordRules[record rules]
    streamRules[stream rules]
    debugRules[debug rules]
    canonical[DTO -> record]
    domainRecords((TemporalRecord))
    recordStage[record xforms]
    streamXforms[stream xforms]
    featureWrap[record -> feature (field select)]
    featureRecords((FeatureRecord))
  end

  dtoStream --> canonical --> domainRecords --> recordStage --> streamXforms --> featureWrap --> featureRecords
  streamsCfg --> mapperEP
  mappersPkg -. ep target .-> mapperEP
  mapperEP -. build_mapper_from_spec .-> registryMappers
  registryMappers --> canonical
  streamsCfg --> recordRules
  streamsCfg --> streamRules
  streamsCfg --> debugRules
  registryRecordOps --> recordRules
  registryStreamOps --> streamRules
  registryDebugOps --> debugRules
  recordRules --> recordStage
  streamRules --> streamXforms
  debugRules --> streamXforms

  subgraph Dataset shaping
    featureSpec[feature cfg]
    groupBySpec[group_by]
    streamRefs[record_stream ids]
    featureTrans[feature/seq xforms]
    sequenceStream((seq/features))
    vectorStage[vector assembly]
    vectorSamples((samples))
  end

  datasetCfg --> featureSpec
  datasetCfg --> groupBySpec
  datasetCfg --> streamRefs
  streamRefs -.->|build_feature_pipeline| registryStreamSources
  registryStreamSources -.->|open_source_stream| sourceNode
  featureRecords --> regularization --> featureTrans --> sequenceStream --> vectorStage --> vectorSamples
  featureSpec -. scale/sequence .-> featureTrans
  groupBySpec -. cadence .-> vectorStage

  subgraph Postprocess
    vectorTransforms[vector xforms]
    postprocessNode[postprocess]
  end

  postprocessCfg --> vectorTransforms -. drop/fill .-> postprocessNode
  vectorStage --> postprocessNode
```
