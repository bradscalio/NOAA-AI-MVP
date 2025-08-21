# NOAA-AI-MVP

EPIC-aligned AI on Red Hat OpenShift: ingest NOAA NWM/HRRR, train XGBoost, serve via KServe.

## Quickstart
- Provision OpenShift (ROSA) and install Operators: OpenShift AI, Pipelines, GitOps, AMQ Streams, ODF.
- Apply Kafka topics, Tekton pipeline, and KServe manifests under `kafka/`, `tekton/`, `kserve/`.
- Build and push ETL container (or let GitHub Actions do it).
- Use the **Student Labs** in `/docs/LABS.md`.

## Data Sources
- NOAA HRRR (public S3)
- NOAA NWM (public S3)
- NWPS API (flood thresholds)

## License
MIT
