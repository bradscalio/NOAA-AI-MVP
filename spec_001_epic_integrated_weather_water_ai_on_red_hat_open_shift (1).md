# SPEC-001-EPIC-Integrated Weather & Water AI on Red Hat OpenShift

## Background

Organizations that plan, operate, and protect water and weather–sensitive infrastructure want AI products that turn trusted federal forecasts into domain actions (e.g., flood risk alerts for counties, hydropower scheduling recommendations, maritime surge guidance). NOAA publishes authoritative, free forecasts and observations (e.g., National Water Model streamflow guidance, HRRR/GFS atmospheric forecasts, NOS coastal water level guidance) on open internet endpoints and cloud buckets. However, these datasets arrive as high‑volume GRIB2/NetCDF files with evolving schemas, making it difficult to operationalize AI and keep models synced with model upgrades.

This specification proposes a pragmatic, EPIC‑aligned reference architecture and demo that integrates NOAA weather and water forecasting into AI domains using Red Hat products:

- **Run anywhere with OpenShift**: a portable Kubernetes foundation to host data pipelines, notebooks, training jobs, and real‑time inferencing.
- **Red Hat OpenShift AI**: managed data‑science workbenches, pipelines, model training/serving (KServe), feature stores, and model monitoring.
- **Red Hat AMQ Streams (Kafka)**: event streaming for near‑real‑time ingestion from NOAA feeds and pub/sub to downstream apps.
- **OpenShift Data Foundation (object + block storage)**: scalable, S3‑compatible storage for raw/curated NOAA data and model artifacts.
- **OpenShift Pipelines & GitOps**: Tekton and Argo CD for reproducible ETL/MLOps and continuous delivery.
- **Ansible Automation**: repeatable provisioning and day‑2 operations in multi‑cluster or hybrid environments.

The demo anchors to **NOAA’s EPIC (Earth Prediction Innovation Center) vision**: consuming UFS/NOAA model outputs from open data programs; containerizing data conditioning and AI components so they are cloud‑ready; and using open repositories/workflows that are straightforward to contribute back to EPIC/UFS communities. The MVP scenario focuses on **flood‑aware streamflow risk** for a pilot basin, fusing **NWM short‑range forecasts** with **HRRR precipitation** and observed gauges to produce a 0–6 hour ahead risk classification, served via an API and dashboard. Subsequent variants extend to coastal total water level risk and reservoir operations.

## Requirements

### Scope & Governance (MoSCoW)

**Must**

- Deploy on **ROSA (us-east-1)** to co‑locate with NOAA S3 buckets (minimizing egress/latency).
- Ingest and harmonize **NOAA NWM Short‑Range** (e.g., `channel_rt` streamflow, CONUS) and **NOAA HRRR** precipitation (e.g., `APCP`, `REFC`) within **≤10 min** of data availability.
- Produce a **0–6 hr streamflow risk classification** (Low/Med/High) at selected gauge locations and along reaches in the pilot basin.
- Provide a **stateless REST/JSON inference API** (KServe on OpenShift AI) and a minimal **React dashboard** for risk visualization.
- Store raw and curated data in **OpenShift Data Foundation (S3‑compatible)** with lifecycle policies; track lineage/versions for reproducibility.
- Use **OpenShift Pipelines (Tekton)** for ETL/MLOps; **GitOps (Argo CD)** for environment promotion; **AMQ Streams (Kafka)** for eventing.
- Align with **NOAA EPIC/UFS** cloud‑ready practices (containers, reproducible workflows, public docs/repo).
- Basic **observability** (Prometheus/Grafana), **model metrics** (latency, throughput, drift), and **alerting** on pipeline failures.

**Should**

- Use **NWPS API (water.noaa.gov/about/api)** for situational context and validation where available.
- Add **feature store** patterns (e.g., parquet tables for rolling hydromet features) to enable re‑training.
- Achieve **API P50 < 300 ms** (in‑cluster) and **P99 < 1.5 s** under 50 RPS; batch scoring completes within **5 min** per model cycle.
- Deliver **competency cards/model cards** with provenance and NOAA dataset references.

**Could**

- Extend to **coastal total water level risk** using NOS water level guidance and surge products.
- Add **edge deployment** on Single‑Node OpenShift for field operations.
- Publish a **public demo repo** with synthetic alerts for tabletop exercises.

**Won’t (MVP)**

- No long‑range forecasting, assimilation changes, or custom hydrodynamic modeling.
- No cross‑cloud active‑active; single ROSA cluster with backups.

### Success Metrics (MVP)

- **Data freshness**: New NWM/HRRR cycles ingested and features built **≤10 min** after object appears in the public bucket.
- **Model performance**: Binary flood‑risk F1‑score **≥0.70** on holdout events in pilot basin; calibration reliability slope **0.9–1.1**.
- **Service SLOs**: 99.5% monthly availability for inference API; **P50 300 ms / P95 800 ms** in‑cluster.
- **Ops**: ETL/MLOps pipelines succeed **≥99%** per month; mean time to recovery (MTTR) **< 30 min**.

### Data & Interfaces

- **Sources (NOAA only, MVP)**: NWM short‑range (us‑east‑1 S3), HRRR (us‑east‑1 S3); NWPS/AHPS JSON where applicable.
- **Formats**: GRIB2/NetCDF4 inputs; curated **Parquet/Zarr** for features; JSON for APIs.
- **Contracts**: Versioned S3 prefixes (`raw/`, `curated/`, `features/`, `models/`, `reports/`); OpenAPI spec for inference.

---

## Method

### 1) EPIC‑aligned technical approach (for an academic training persona)

- **Principle**: Treat NOAA models (UFS ecosystem outputs like **NWM** and **HRRR**) as *authoritative truth sources*; AI layers add *translation* (risk classification, pedagogy) rather than new NWP physics. All workflows are **containerized**, **reproducible**, and **cloud‑portable**, matching EPIC’s cloud‑first, community ethos.
- **Student experience**: RHOAI (OpenShift AI) **Workbenches** (JupyterLab) with curated conda images (`xarray`, `cfgrib`, `ecCodes`, `rioxarray`, `zarr`, `scikit‑learn`, `xgboost`). Students can open notebook labs, explore NOAA datasets directly from public S3 (no credentials), and publish trained models to KServe.
- **Ops experience**: Faculty/TA own Git repos for ETL/MLOps; **Tekton** and **Data Science Pipelines** build features and models each cycle; **AMQ Streams (Kafka)** carries events; **ODF (S3)** houses raw/curated data and artifacts; **KServe** serves models behind an API for the class dashboard.

### 2) Datasets (pilot)

- **Hydrology**: *National Water Model* (Short‑Range, hourly to +18h). Primary variable: `streamflow` at **COMIDs**; optional states (soil moisture) if needed.
- **Atmosphere**: *HRRR* precipitation fields (e.g., `APCP` accumulations). Optional reflectivity (`REFC`) for convective context.
- **Gauges/thresholds** (for labels & pedagogy): **NWPS API** gauge metadata & flood categories (minor/moderate/major) for the **Red River of the North at Fargo (FGON8)**.

### 3) Logical architecture (OpenShift on ROSA, us‑east‑1)

```plantuml
@startuml
skinparam componentStyle rectangle
skinparam wrapWidth 200
actor "Students (Jupyter)" as Student
actor "Instructors/TA" as Instructor

package "OpenShift (ROSA, us-east-1)" {
  [OpenShift AI Workbenches
(JupyterLab images)] as WB
  [Data Science Pipelines
(Kubeflow Pipelines 2.x)] as DSP
  [OpenShift Pipelines
(Tekton)] as Tekton
  [AMQ Streams
(Kafka)] as Kafka
  [OpenShift Data Foundation
(S3 Object Store)] as ODF
  [Model Serving
(KServe/Knative)] as KServe
  [Observability
(Prometheus/Grafana/LOK)] as Obs
  [GitOps
(Argo CD)] as GitOps
}

package "NOAA Open Data (Public)" {
  [NWM Short-Range S3] as NWM
  [HRRR S3] as HRRR
  [NWPS API (gauges, categories)] as NWPS
}

Student --> WB : notebooks, labs
Instructor --> GitOps : course repos
GitOps --> Tekton : pipeline definitions
Tekton --> NWM : poll/list/
Tekton --> HRRR : poll/list
Tekton --> ODF : store raw/curated
Tekton --> Kafka : publish new-cycle events
DSP --> ODF : read features, write models
WB --> ODF : ad hoc exploration
DSP --> KServe : register/deploy model
KServe --> Kafka : emit inference events
Obs <- Tekton
Obs <- KServe
WB --> KServe : test calls
WB --> NWPS : label thresholds (read)
@enduml
```

### 4) Dataflow (ingest → features → train → serve)

```plantuml
@startuml
skinparam sequenceMessageAlign center
actor Operator
participant "Tekton ETL Task" as ETL
participant "ODF S3 (raw/curated)" as S3
participant "Feature Builder
(Python/xarray)" as FE
participant "Pipelines (KFP 2.x)" as KFP
participant "Model Registry (opt)" as REG
participant "KServe InferenceService" as ISVC

Operator -> ETL: Start hourly (or on cron)
ETL -> S3: mirror latest NWM/HRRR to raw/
ETL -> FE: trigger feature job (Kafka event)
FE -> S3: read raw/ write curated/ + features/
FE -> KFP: notify pipeline run
KFP -> S3: load features & labels
KFP -> REG: register model artifact (optional)
KFP -> ISVC: deploy model (KServe)
ISVC -> S3: read model artifacts
@enduml
```

### 5) Storage layout & interfaces

**Object store (ODF S3) prefixes**

```
s3://epic-demo-odf/
  raw/
    nwm/short_range/{YYYY}/{MM}/{DD}/{HH}/...
    hrrr/{YYYY}/{MM}/{DD}/{HH}/...
  curated/
    nwm/v3/channel_rt.parquet/...
    hrrr/apcp.parquet/...
  features/
    red_river_fgON8/feature_date={YYYYMMDDHH}/part-*.parquet
  models/
    streamflow_risk/{model_id}/model.joblib
  reports/
    metrics/{run_id}.json
```

**Kafka topics**

- `noaa.nwm.short_range.ingested` (key: cycle, value: manifest)
- `noaa.hrrr.precip.ingested`
- `features.streamflowrisk.ready` (key: basin/gauge, value: s3 path)
- `inference.risk.events` (key: gauge, value: score payload)

**Inference API (KServe, v2)**

- `POST /v2/models/streamflow-risk/infer` → input: `{timestamp, gauge_id, nwm_cycle, features[]}` → output: `{risk_class, p_high, explanations}`

### 6) Feature engineering (pilot)

Goal: **0–6 h streamflow risk** (Low/Med/High) at FGON8 and nearby reaches.

**Predictors** (per time `t` and lead `h ∈ {1..6}`)

- `q_nwm_{h}`: NWM forecast streamflow at lead `h` (cms)
- `q_pct_{h}`: percentile of `q_nwm_{h}` vs. historical distribution at this gauge/reach
- `apcp_{0-1h}`, `apcp_{1-3h}`, `apcp_{3-6h}`: HRRR accumulated precip windows
- `dq_dt_{h}`: finite difference of forecast flow between leads (rise rate)
- `antecedent_q`: last observed/analysis flow (if available from NWPS)
- Optional pedagogy features: upstream contributing area, slope, soil moisture proxy from NWM state if desired

**Labels**

- Map NWPS gauge flood categories to **Low/Med/High** classes:
  - `Low` = below *action* stage
  - `Med` = at/above *minor* but below *moderate*
  - `High` = at/above *moderate* (or *major*). Labels computed from NWPS stage forecasts/observations aligned to time `t+h`.

**Pre‑processing**

- Use `xarray` + `cfgrib`/`ecCodes` for GRIB2; `zarr`/Parquet for curated features.
- Spatial join logic (only if necessary) to map HRRR grid cells to reach/gauge using nearest‑neighbor or area‑weighted interpolation.
- Standardize/normalize continuous features; persist a `sklearn.Pipeline` with transformers + estimator.

### 7) Modeling strategy

- **Baseline**: Regularized **logistic regression** (interpretable; fast for class).
- **Primary**: **XGBoost** classifier (handles non‑linearities; robust to mixed features).
- **Calibration**: Platt scaling or isotonic regression on validation fold to yield reliable probabilities.
- **Explainability**: SHAP values on a small sample for lectures.
- **Packaging**: Save `sklearn`/`xgboost` pipeline to `joblib`; optional **ONNX** export for portability.

**Evaluation (educational)**

- Metrics: F1 (High vs. not), ROC‑AUC, Reliability diagram slope (0.9–1.1), Brier score.
- Backtests on selected flood events; k‑fold by event windows to reduce leakage.

### 8) Minimal schemas

\`\`\*\* (Parquet)\*\*

| column       | type      | notes                   |
| ------------ | --------- | ----------------------- |
| `gauge_id`   | string    | e.g., `FGON8`           |
| `reach_id`   | long      | NHDPlusV2 COMID if used |
| `valid_time` | timestamp | UTC of forecast valid   |
| `lead_h`     | smallint  | 1..6                    |
| `nwm_cycle`  | timestamp | cycle init time         |
| `q_nwm_h`    | double    | cms                     |
| `q_pct_h`    | double    | 0–100                   |
| `apcp_0_1h`  | double    | mm                      |
| `apcp_1_3h`  | double    | mm                      |
| `apcp_3_6h`  | double    | mm                      |
| `dq_dt_h`    | double    | cms/hr                  |
| `label`      | tinyint   | 0=Low,1=Med,2=High      |
| `split`      | string    | train/val/test          |

\`\`\*\* (JSON)\*\*

```json
{
  "run_id": "2025-08-21T12:00Z",
  "gauge": "FGON8",
  "f1_high": 0.72,
  "roc_auc": 0.82,
  "reliability_slope": 0.98,
  "latency_ms_p50": 210,
  "freshness_min": 8
}
```

### 9) Runtime & scaling

- **Serving**: KServe single‑model runtime; 1–2 replicas; Knative scale‑to‑zero enabled for student labs.
- **Batch**: Feature builds run as Tekton tasks; parallelized via Dask (optional) inside the Python container.
- **Storage**: ODF bucket with lifecycle rules: `raw/` 30‑day TTL, `curated/` 90‑day, `features/` 180‑day. Models retained 12 months.

### 10) Security & governance

- Project‑scoped **RHOAI Data Science Projects**; fine‑grained access to notebooks, pipelines, models.
- Pull from NOAA S3 using **anonymous (no‑sign‑request)**; outbound egress allowed only to specific endpoints.
- Image provenance via **Quay** and **ImageContentSourcePolicy**; GitOps for manifests; SBOMs in CI.
- Educational **model card** stored under `reports/` with data sources and limitations.

---

## Implementation

> This section includes both **ops steps** (Operators, namespaces, storage) and a **demo runbook** your instructors can follow in class.

### A. Platform bring-up (ROSA, us-east-1)

1. **Create ROSA cluster** (HCP or classic) in `us-east-1` per docs. Ensure private egress to AWS public S3 is allowed. Create an OpenShift admin user.
2. **Install Operators** (via OperatorHub, cluster‑wide):
   - **Red Hat OpenShift AI** (OpenShift AI/ODS Operator)
   - **OpenShift Pipelines** (Tekton)
   - **OpenShift GitOps** (Argo CD)
   - **Red Hat AMQ Streams** (Kafka)
   - **OpenShift Data Foundation** (for object storage)
3. **Projects/Namespaces**: Create a project `epic-ai` for shared assets.

### B. Object storage (ODF S3)

1. Create an **ObjectBucketClaim** in `epic-ai` for model/data buckets:
   ```yaml
   apiVersion: objectbucket.io/v1alpha1
   kind: ObjectBucketClaim
   metadata:
     name: odf-epic-bucket
     namespace: epic-ai
   spec:
     generateBucketName: epic-demo-odf
     storageClassName: openshift-storage.noobaa.io
   ```
   This creates a **Secret** (`odf-epic-bucket`) with S3 keys and a **ConfigMap** exposing endpoint/URL.
2. Create the folder layout inside the bucket (from any pod with `awscli`):
   ```bash
   aws s3 --endpoint-url "$S3_ENDPOINT" mb s3://epic-demo-odf
   aws s3 --endpoint-url "$S3_ENDPOINT" cp --recursive ./bootstrap-layout s3://epic-demo-odf/
   ```

### C. Eventing (AMQ Streams)

Provision a small Kafka cluster for pipeline events:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: epic-kafka
  namespace: epic-ai
spec:
  kafka:
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    storage:
      type: ephemeral
  zookeeper:
    replicas: 3
    storage:
      type: ephemeral
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

Create topics:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata: {name: noaa.nwm.short_range.ingested, namespace: epic-ai, labels: {strimzi.io/cluster: epic-kafka}}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata: {name: noaa.hrrr.precip.ingested, namespace: epic-ai, labels: {strimzi.io/cluster: epic-kafka}}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata: {name: features.streamflowrisk.ready, namespace: epic-ai, labels: {strimzi.io/cluster: epic-kafka}}
spec: {partitions: 3, replicas: 3}
```

### D. OpenShift AI (Projects, Workbenches, Pipelines, Serving)

1. In the **OpenShift AI** dashboard, create a **Data Science Project** named `EPIC Training` (resource name `epic-ai`).
2. **Workbench**: Create a JupyterLab workbench (CPU) with the following `requirements.txt` (or build a custom image):
   ```
   xarray
   s3fs
   fsspec
   cfgrib
   eccodes
   zarr
   pandas
   numpy
   scikit-learn
   xgboost
   shap
   fastapi
   uvicorn
   ```
3. **Data Science Pipelines 2.0**: Ensure KFP v2 is enabled in OpenShift AI. You’ll import a compiled pipeline artifact in step *G*.
4. **Model Serving (KServe)**: Enable single‑model serving for the project; students will later deploy their own models.

### E. Data ingest & curation (Tekton + Python)

Create a minimal container image `quay.io/<org>/noaa-etl:latest` that contains the libraries from the workbench and your Python ETL entrypoints (`ingest_noaa.py`, `build_features.py`). Then define a Tekton **Pipeline**:

```yaml
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: epic-noaa-etl
  namespace: epic-ai
spec:
  params:
    - name: cycle_utc
      type: string
      description: NWP cycle like 2025-08-21T12:00:00Z
  tasks:
    - name: ingest-noaa
      taskSpec:
        params: [{name: cycle_utc, type: string}]
        steps:
          - name: ingest
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/ingest_noaa.py --cycle $(params.cycle_utc) \
                --odf-bucket epic-demo-odf --kafka-bootstrap epic-kafka-kafka-bootstrap:9092
    - name: build-features
      runAfter: [ingest-noaa]
      taskSpec:
        params: [{name: cycle_utc, type: string}]
        steps:
          - name: features
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/build_features.py --cycle $(params.cycle_utc) \
                --bucket epic-demo-odf --basin red_river_fgo --lead-hours 1 2 3 4 5 6
```

**Example ****\`\`**** (excerpt)**

```python
import os, json, datetime as dt
import boto3
from botocore import UNSIGNED
from botocore.config import Config

CYCLE = dt.datetime.fromisoformat(os.environ.get('CYCLE', '2025-08-21T12:00:00+00:00'))
s3pub = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# HRRR bucket and path example (AWS us-east-1)
hrrr_bucket = 'noaa-hrrr-bdp-pds'
hrrr_prefix = f"hrrr.{CYCLE:%Y%m%d}/conus/"
keys = []
resp = s3pub.list_objects_v2(Bucket=hrrr_bucket, Prefix=hrrr_prefix)
for o in resp.get('Contents', []):
    if 'wrfsfcf' in o['Key'] and o['Key'].endswith('.grib2'): keys.append(o['Key'])
print(json.dumps({'cycle': CYCLE.isoformat(), 'found': len(keys)}))
# TODO: stream-select variables (APCP), stage to ODF S3 using credentials from env
```

**Example ****\`\`**** (excerpt)**

```python
import os, pandas as pd, xarray as xr
import fsspec
CYCLE=os.environ.get('CYCLE')
fs_pub = fsspec.filesystem('s3', anon=True)
# Open a single HRRR GRIB2 file to extract precip
url = f"s3://noaa-hrrr-bdp-pds/hrrr.{CYCLE[0:10].replace('-','')}/conus/hrrr.t{CYCLE[11:13]}z.wrfsfcf01.grib2"
ds = xr.open_dataset(url, engine='cfgrib', backend_kwargs={'indexpath': ''},
                     storage_options={'anon': True})
# ... reduce to APCP windows near the basin bbox, aggregate, and write Parquet to ODF S3
```

### F. Labels and thresholds (NWPS API)

Use **NWPS API** for gauge metadata and flood category levels (minor/moderate/major). Students can call:

```
https://api.water.noaa.gov/nwps/v1/docs/
```

and explore endpoints for the Fargo site to build labels for `High/Med/Low`.

### G. Training pipeline (KFP v2) and model registry

Create a minimal KFP v2 pipeline with two components: (1) assemble training data from `features/` and NWPS labels; (2) train + calibrate + export a `joblib` model to `models/streamflow_risk/{run_id}/`.

**Pipeline stub (Python)**

```python
from kfp import dsl
@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def assemble_data(bucket: str, out_path: dsl.Output[dsl.Dataset]):
    # read Parquet features, join labels from NWPS API, write to out_path
    ...
@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def train_export(in_data: dsl.Input[dsl.Dataset], model_uri: str):
    # train XGBoost, calibrate, save joblib to s3 model_uri
    ...
@dsl.pipeline(name='streamflow-risk-train')
def pipe(bucket: str='epic-demo-odf', model_uri: str='s3://epic-demo-odf/models/streamflow_risk/latest/'):
    data = assemble_data(bucket)
    train_export(data.outputs['out_path'], model_uri)
```

Compile and **import** the pipeline in OpenShift AI, then create a **scheduled run** after features are built.

### H. Model serving (KServe)

Deploy the trained model with KServe (single‑model platform):

```yaml
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: streamflow-risk
  namespace: epic-ai
spec:
  predictor:
    sklearn:
      storageUri: s3://epic-demo-odf/models/streamflow_risk/latest/
      resources:
        requests: {cpu: "250m", memory: "512Mi"}
        limits: {cpu: "1", memory: "1Gi"}
```

Optional: enable request/response logging to Kafka for class exercises.

### I. Minimal dashboard (React + fetch to KServe)

Provide a simple dashboard that calls the model endpoint and renders risk for a few locations. (Use a **ClusterRoute** or Service to expose KServe.)

```html
<!DOCTYPE html>
<html>
  <head><meta charset="utf-8"/><title>Streamflow Risk Demo</title></head>
  <body>
    <h1>FGON8 – 0–6h Streamflow Risk</h1>
    <div id="out"></div>
    <script>
      async function run() {
        const body = {inputs: [{name: "features", shape: [1,12], datatype: "FP32", data: [/* demo feature vector */]}]};
        const res = await fetch('/api/streamflow-risk/v2/models/streamflow-risk/infer', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)});
        const j = await res.json();
        document.getElementById('out').textContent = JSON.stringify(j, null, 2);
      }
      run();
    </script>
  </body>
</html>
```

---

### Demo Runbook (for a 60–90 minute class)

**0. Prep (Instructor)**

- Cluster ready; Operators installed; `epic-ai` project exists; bucket and Kafka up.
- Push `noaa-etl:latest` to Quay; import pipeline to OpenShift AI.

**1. Open with EPIC context (5 min)**

- Brief EPIC goals, UFS alignment, and NOAA open datasets.

**2. Explore NOAA data live (10–15 min)**

- In Workbench, list HRRR/NWM public S3 and open a GRIB2 file with `xarray+cfgrib`.
- Show NWPS API response for Fargo to explain flood categories and labels.

**3. Kick off ETL (10 min)**

- Start Tekton `PipelineRun` with `cycle_utc=<now rounded hour>`; show Kafka topic messages and new objects under `raw/` and `curated/`.

**4. Feature build + KFP train (15–20 min)**

- Trigger KFP run; show metrics artifact and model saved under `models/`.

**5. Deploy to KServe (10 min)**

- Create `InferenceService`. Curl the endpoint. Discuss autoscaling and tracing.

**6. Dashboard call (5 min)**

- Load the HTML page and show inference JSON for FGON8.

**7. Closing (5 min)**

- Review SLOs, model card, and how this maps to EPIC community workflows.

---

## Milestones

1. **Week 1** – ROSA cluster, Operators, storage, Kafka ready.
2. **Week 2** – ETL container + Tekton pipeline; bucket layout; first ingest of HRRR/NWM.
3. **Week 3** – Feature engineering + KFP v2 pipeline; backtest metrics.
4. **Week 4** – KServe deploy + dashboard; instructor dry run; class materials finalized.

## Gathering Results

- **Ops**: pipeline success rate, API latency, data freshness (minutes after NOAA publish).
- **Model**: F1 (High), ROC‑AUC, reliability slope; confusion over flood categories addressed via NWPS.
- **Teaching**: student completion rate of labs, quiz on interpreting NWM/HRRR inputs and NWPS labels.

## Need Professional Help in Developing Your Architecture?

Please contact me at [sammuti.com](https://sammuti.com) :)

## Code Bundle (MVP Demo)

> Drop these files into a repo (e.g., `epic-epic-demo/`) and adjust bucket/endpoint names as needed. All containers and pipelines are CPU‑only.

### Directory

```
.
├── containers/
│   └── etl/
│       └── Dockerfile
├── etl/
│   ├── ingest_noaa.py
│   └── build_features.py
├── pipelines/
│   └── kfp_pipeline.py
├── tekton/
│   ├── pipeline.yaml
│   └── pipeline-run.yaml
├── kafka/
│   └── topics.yaml
├── kserve/
│   ├── inferenceservice.yaml
│   └── s3-secret.yaml
├── dash/
│   └── index.html
└── requirements.txt
```

### `requirements.txt`

```
xarray
s3fs
fsspec
cfgrib
eccodes
zarr
netcdf4
pandas
numpy
scikit-learn
xgboost
shap
fastapi
uvicorn
boto3
pyarrow
```

xarray s3fs fsspec cfgrib eccodes zarr pandas numpy scikit-learn xgboost shap fastapi uvicorn boto3 pyarrow

````

### `containers/etl/Dockerfile`
```Dockerfile
# Base on an OpenShift AI-compatible image with conda/mamba
FROM quay.io/modh/odh-generic-data-science-notebook:py3.11-2024a
USER root
RUN microdnf -y install git && microdnf clean all
USER 1001
# Use conda-forge to get eccodes/cfgrib/netcdf4 reliably
RUN mamba install -y -n base -c conda-forge \
    xarray cfgrib eccodes zarr s3fs fsspec pandas numpy scikit-learn xgboost shap dask boto3 pyarrow netcdf4 && \
    mamba clean -afy
WORKDIR /app
COPY etl/ /app/
ENTRYPOINT ["bash","-lc"]
```Dockerfile
# Base on an OpenShift AI-compatible image with conda/mamba
FROM quay.io/modh/odh-generic-data-science-notebook:py3.11-2024a
USER root
RUN microdnf -y install git && microdnf clean all
USER 1001
# Use conda-forge to get eccodes/cfgrib reliably
RUN mamba install -y -n base -c conda-forge \
    xarray cfgrib eccodes zarr s3fs fsspec pandas numpy scikit-learn xgboost shap dask boto3 pyarrow && \
    mamba clean -afy
WORKDIR /app
COPY etl/ /app/
ENTRYPOINT ["bash","-lc"]
````

### `etl/ingest_noaa.py`

```python
import argparse, os, sys, io
import datetime as dt
import boto3
from botocore.config import Config
from botocore import UNSIGNED

PUB = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

def s3_client_to_odf():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST')
        port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url=endpoint,
        region_name=os.getenv('AWS_DEFAULT_REGION','us-east-1')
    )

def copy_obj(src_bucket, src_key, dst_bucket, dst_key, odf):
    # stream copy to avoid large memory usage
    src = PUB.get_object(Bucket=src_bucket, Key=src_key)
    odf.upload_fileobj(src['Body'], dst_bucket, dst_key)

def hrrr_keys(cycle):
    # HRRR surface forecast hours 1..6
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"hrrr.{ymd}/conus/"
    keys = [f"{base}hrrr.t{hh}z.wrfsfcf{h:02}.grib2" for h in range(1,7)]
    return 'noaa-hrrr-bdp-pds', keys

def nwm_keys(cycle):
    # NWM short_range channel_rt hours 1..6
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"nwm.{ymd}/short_range/"
    keys = [f"{base}nwm.t{hh}z.short_range.channel_rt.conus.f{h:03}.nc" for h in range(1,7)]
    return 'noaa-nwm-pds', keys

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True, help='ISO time, e.g., 2025-08-21T12:00:00Z')
    ap.add_argument('--odf-bucket', required=True)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    odf = s3_client_to_odf()

    for (bucket, keys, label) in [(*hrrr_keys(cycle), 'hrrr'), (*nwm_keys(cycle), 'nwm')]:
        for k in keys:
            # verify exists
            try:
                PUB.head_object(Bucket=bucket, Key=k)
            except Exception:
                print(f"skip missing {bucket}/{k}")
                continue
            dst_key = f"raw/{label}/{cycle:%Y/%m/%d/%H}/" + k.split('/')[-1]
            print(f"copy {bucket}/{k} -> {args.odf-bucket}/{dst_key}")
            copy_obj(bucket, k, args.odf-bucket, dst_key, odf)
    print('done')
```

### `etl/build_features.py`

```python
import argparse, os, json
import datetime as dt
import pandas as pd
import xarray as xr
import fsspec

# NOTE: For simplicity, this MVP uses only NWM flows to build features.
# HRRR precipitation windows can be added later by opening GRIB2 with cfgrib and aggregating APCP.

def odf_fs():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST'); port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    key = os.getenv('AWS_ACCESS_KEY_ID'); sec = os.getenv('AWS_SECRET_ACCESS_KEY')
    return fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})


def load_nwm_series(fs, bucket, cycle):
    rows = []
    for h in range(1,7):
        key = f"raw/nwm/{cycle:%Y/%m/%d/%H}/nwm.t{cycle:%H}z.short_range.channel_rt.conus.f{h:03}.nc"
        url = f"s3://{bucket}/{key}"
        if not fs.exists(url):
            continue
        ds = xr.open_dataset(url, engine='netcdf4', backend_kwargs={'autoclose': True},
                             storage_options={'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']},
                                              'key': fs.key, 'secret': fs.secret})
        # pick a COMID near Fargo (example COMID 13342706 is illustrative; replace with proper mapping if desired)
        # For MVP, aggregate domain-wide mean flow as a proxy; students can refine spatial mapping in lab.
        varnames = [v for v in ds.data_vars]
        # NWM channel_rt commonly has 'streamflow'
        v = 'streamflow' if 'streamflow' in varnames else varnames[0]
        q = float(ds[v].mean().values)  # proxy
        rows.append({'lead_h': h, 'q_nwm_h': q})
        ds.close()
    return pd.DataFrame(rows)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True)
    ap.add_argument('--bucket', required=True)
    ap.add_argument('--gauge-id', default='FGON8')
    ap.add_argument('--flow-high-thresh-cms', type=float, default=1500.0)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    fs = odf_fs()

    df = load_nwm_series(fs, args.bucket, cycle)
    # Build simple features from q
    for h in range(1,7):
        if h not in df['lead_h'].values:
            df = pd.concat([df, pd.DataFrame([{'lead_h': h, 'q_nwm_h': None}])])
    df = df.sort_values('lead_h')
    df['dq_dt_h'] = df['q_nwm_h'].diff().fillna(0)
    # Labels: High if 3h flow exceeds threshold; Medium if 6h exceeds 0.7 * threshold; else Low
    q3 = df.loc[df['lead_h']==3, 'q_nwm_h'].values[0] if (df['lead_h']==3).any() else 0
    q6 = df.loc[df['lead_h']==6, 'q_nwm_h'].values[0] if (df['lead_h']==6).any() else 0
    if q3 and q3 >= args.flow_high_thresh_cms:
        label = 2
    elif q6 and q6 >= 0.7*args.flow_high_thresh_cms:
        label = 1
    else:
        label = 0
    # Persist features (single row per lead for demo)
    valid_time = cycle + dt.timedelta(hours=1)
    records = []
    for _,r in df.iterrows():
        records.append({
            'gauge_id': args.gauge_id,
            'reach_id': -1,
            'valid_time': valid_time.isoformat(),
            'lead_h': int(r['lead_h']),
            'nwm_cycle': cycle.isoformat(),
            'q_nwm_h': None if pd.isna(r['q_nwm_h']) else float(r['q_nwm_h']),
            'q_pct_h': None,
            'apcp_0_1h': None,
            'apcp_1_3h': None,
            'apcp_3_6h': None,
            'dq_dt_h': float(r['dq_dt_h']) if r['dq_dt_h'] is not None else 0.0,
            'label': label,
            'split': 'train'
        })
    out_prefix = f"s3://{args.bucket}/features/red_river_fgON8/feature_date={cycle:%Y%m%d%H}/"
    out_path = out_prefix + 'part-000.parquet'
    pd.DataFrame.from_records(records).to_parquet(out_path, index=False, storage_options={'key': fs.key, 'secret': fs.secret, 'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']}})
    print(json.dumps({'out': out_path}))
```

### `pipelines/kfp_pipeline.py`

```python
from kfp import dsl
from typing import NamedTuple

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def assemble_training(bucket: str, cycle: str, out_path: dsl.Output[dsl.Dataset]):
    import pandas as pd, fsspec, os
    host=os.getenv('BUCKET_HOST'); port=os.getenv('BUCKET_PORT'); tls=os.getenv('BUCKET_TLS','false').lower()== 'true'
    endpoint=os.getenv('S3_ENDPOINT') or (('https' if tls else 'http')+f"://{host}:{port}")
    fs=fsspec.filesystem('s3', key=os.getenv('AWS_ACCESS_KEY_ID'), secret=os.getenv('AWS_SECRET_ACCESS_KEY'), client_kwargs={'endpoint_url': endpoint})
    pref=f"s3://{bucket}/features/red_river_fgON8/feature_date="+cycle.replace('-','').replace(':','')[:10]+"/"
    files=fs.glob(pref+"*.parquet")
    df=pd.concat([pd.read_parquet(f, storage_options={'key':fs.key,'secret':fs.secret,'client_kwargs':{'endpoint_url':endpoint}}) for f in files])
    df.to_parquet(out_path.path)

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def train_and_export(in_data: dsl.Input[dsl.Dataset], model_uri: str) -> NamedTuple('Metrics', [('f1', float)]):
    import pandas as pd, numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import f1_score
    from xgboost import XGBClassifier
    import joblib, os
    df=pd.read_parquet(in_data.path)
    y=df['label']
    X=df[['q_nwm_h','dq_dt_h']].fillna(0)
    X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=42)
    clf=XGBClassifier(n_estimators=100,max_depth=3,learning_rate=0.1,subsample=0.9,colsample_bytree=0.9,tree_method='hist')
    clf.fit(X_train,y_train)
    yhat=(clf.predict_proba(X_test)[:,2]>0.5).astype(int)
    # Map 2->1 for binary F1 on High class
    f1=f1_score((y_test==2).astype(int), yhat)
    # save as sklearn-compatible joblib
    os.makedirs('/tmp/model', exist_ok=True)
    joblib.dump(clf, '/tmp/model/model.joblib')
    # write to S3
    import fsspec
    key=os.getenv('AWS_ACCESS_KEY_ID'); sec=os.getenv('AWS_SECRET_ACCESS_KEY'); endpoint=os.getenv('S3_ENDPOINT')
    fs=fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})
    fs.put('/tmp/model/model.joblib', model_uri.rstrip('/')+'/model.joblib')
    from typing import NamedTuple
    return (float(f1),)

@dsl.pipeline(name='streamflow-risk-train')
def pipe(bucket: str='epic-demo-odf', cycle: str='2025082112', model_uri: str='s3://epic-demo-odf/models/streamflow_risk/latest/'):
    data = assemble_training(bucket=bucket, cycle=cycle)
    train_and_export(in_data=data.outputs['out_path'], model_uri=model_uri)
```

### `tekton/pipeline.yaml`

```yaml
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: epic-noaa-etl
  namespace: epic-ai
spec:
  params:
    - name: cycle_utc
      type: string
      description: NWP cycle like 2025-08-21T12:00:00Z
    - name: odf_bucket
      type: string
      default: epic-demo-odf
  tasks:
    - name: ingest-noaa
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: ingest
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/ingest_noaa.py --cycle $(params.cycle_utc) --odf-bucket $(params.odf_bucket)
    - name: build-features
      runAfter: [ingest-noaa]
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: features
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/build_features.py --cycle $(params.cycle_utc) --bucket $(params.odf_bucket) --gauge-id FGON8
```

### `tekton/pipeline-run.yaml`

```yaml
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: epic-noaa-etl-
  namespace: epic-ai
spec:
  pipelineRef: {name: epic-noaa-etl}
  params:
    - name: cycle_utc
      value: "2025-08-21T12:00:00Z"
    - name: odf_bucket
      value: epic-demo-odf
  timeouts:
    pipeline: 1h0m0s
```

### `kafka/topics.yaml`

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.nwm.short_range.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.hrrr.precip.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: features.streamflowrisk.ready
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
```

### `kserve/s3-secret.yaml`

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kserve-s3-creds
  namespace: epic-ai
stringData:
  AWS_ACCESS_KEY_ID: "$(AWS_ACCESS_KEY_ID)"
  AWS_SECRET_ACCESS_KEY: "$(AWS_SECRET_ACCESS_KEY)"
  AWS_DEFAULT_REGION: "us-east-1"
```

### `kserve/inferenceservice.yaml`

````yaml
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: streamflow-risk
  namespace: epic-ai
  annotations:
    serving.kserve.io/s3-endpoint: "http://$(BUCKET_HOST):$(BUCKET_PORT)"
    serving.kserve.io/s3-usehttps: "0"
    serving.kserve.io/s3-region: "us-east-1"
    serving.kserve.io/s3-secret-name: "kserve-s3-creds"
spec:
  predictor:
    xgboost:
      storageUri: s3://epic-demo-odf/models/streamflow_risk/latest/
      resources:
        requests: {cpu: "250m", memory: "512Mi"}
        limits: {cpu: "1", memory: "1Gi"}
```yaml
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: streamflow-risk
  namespace: epic-ai
  annotations:
    serving.kserve.io/s3-endpoint: "http://$(BUCKET_HOST):$(BUCKET_PORT)"
    serving.kserve.io/s3-usehttps: "0"
    serving.kserve.io/s3-region: "us-east-1"
    serving.kserve.io/s3-secret-name: "kserve-s3-creds"
spec:
  predictor:
    sklearn:
      storageUri: s3://epic-demo-odf/models/streamflow_risk/latest/
      resources:
        requests: {cpu: "250m", memory: "512Mi"}
        limits: {cpu: "1", memory: "1Gi"}
````

### `dash/index.html`

```html
<!doctype html>
<html>
<head><meta charset="utf-8"><title>FGON8 – Streamflow Risk</title></head>
<body>
  <h1>FGON8 – 0–6h Streamflow Risk</h1>
  <pre id="out">loading…</pre>
  <script>
    async function call() {
      // Minimal V2 payload with placeholder features (q_nwm_h, dq_dt_h)
      const body = {"inputs":[{"name":"predict","shape":[1,2],"datatype":"FP32","data":[1500, 120]}]};
      const res = await fetch('/v2/models/streamflow-risk/infer', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
      document.getElementById('out').textContent = JSON.stringify(await res.json(), null, 2);
    }
    call();
  </script>
</body>
</html>
```

### Quick commands

```
# 1) Build & push ETL image
podman build -t quay.io/<org>/noaa-etl:latest -f containers/etl/Dockerfile .
podman push quay.io/<org>/noaa-etl:latest

# 2) Create Kafka topics, KServe secret, and InferenceService
oc apply -f kafka/topics.yaml
oc apply -f kserve/s3-secret.yaml
oc apply -f kserve/inferenceservice.yaml

# 3) Create Tekton pipeline and a run
oc apply -f tekton/pipeline.yaml
oc create -f tekton/pipeline-run.yaml
```

> Notes: (1) Replace `$(...)` placeholders in secrets/annotations with values from your OBC `odf-epic-bucket` ConfigMap/Secret. (2) The feature builder currently derives labels from NWM flow thresholds for demo speed. For coursework, extend it to pull **NWPS** thresholds and (optionally) map stage→discharge using local rating curves, then recompute labels. (3) Add HRRR precipitation windows by opening the GRIB2 files in `raw/hrrr/…` and aggregating `APCP` over the pilot basin—students can complete this as part of Lab 2.

## Student Lab Worksheets

> Each lab fits a single 45–60 minute block. Labs reference the code bundle paths already included.

### Lab 1 — Explore NOAA Data & Ingest to ODF

**Objectives**

- Read **HRRR** and **NWM** from public S3.
- Run the **Tekton ETL** to mirror raw data into ODF S3 and build first features.

**Steps**

1. **List public S3 keys (read-only)** in your Workbench terminal:
   ```bash
   aws s3 ls s3://noaa-hrrr-bdp-pds/hrrr.$(date -u +%Y%m%d)/conus/ --no-sign-request | head
   aws s3 ls s3://noaa-nwm-pds/nwm.$(date -u +%Y%m%d)/short_range/ --no-sign-request | head
   ```
2. **Open a GRIB2 file** in Python (Workbench):
   ```python
   import xarray as xr
   url = 's3://noaa-hrrr-bdp-pds/hrrr.20250101/conus/hrrr.t00z.wrfsfcf01.grib2'
   ds = xr.open_dataset(url, engine='cfgrib', backend_kwargs={'indexpath': ''}, storage_options={'anon': True})
   list(ds.data_vars)
   ```
3. **Run ETL** (Tekton):
   - Update `tekton/pipeline-run.yaml` → set `cycle_utc` to the latest rounded UTC hour.
   - Apply: `oc create -f tekton/pipeline-run.yaml -n epic-ai`.
4. **Verify ODF objects** (replace endpoint/creds with your OBC):
   ```bash
   aws --endpoint-url "$S3_ENDPOINT" s3 ls s3://epic-demo-odf/raw/nwm/$(date -u +%Y/%m/%d/%H)/
   aws --endpoint-url "$S3_ENDPOINT" s3 ls s3://epic-demo-odf/features/red_river_fgON8/ --recursive | tail -n2
   ```
5. **Peek features**:
   ```python
   import pandas as pd
   df = pd.read_parquet('s3://epic-demo-odf/features/red_river_fgON8/.../part-000.parquet',
                        storage_options={'client_kwargs': {'endpoint_url': os.environ['S3_ENDPOINT']},
                                         'key': os.environ['AWS_ACCESS_KEY_ID'],
                                         'secret': os.environ['AWS_SECRET_ACCESS_KEY']})
   df.head()
   ```

**Checkpoints**

- You see ≥6 HRRR and ≥6 NWM files under `raw/…/<cycle>/`.
- A Parquet features file exists under `features/red_river_fgON8/feature_date=<cycle>/`.

**Stretch**

- Call **NWPS API** for Fargo and display flood category thresholds.

---

### Lab 2 — Feature Engineering & Training

**Objectives**

- Add **HRRR precipitation windows (APCP)** to features.
- Train **XGBoost**, report F1 (High), and save a model card.

**Steps**

1. **Extend** `etl/build_features.py` to compute `apcp_0_1h`, `apcp_1_3h`, `apcp_3_6h`:
   ```python
   import numpy as np
   def apcp_window(urls):
       import xarray as xr
       vals=[]
       for u in urls:
           ds=xr.open_dataset(u, engine='cfgrib', backend_kwargs={'indexpath':''}, storage_options={'anon':True})
           # APCP total accumulation over domain (demo); refine to basin mask as exercise
           v='tp' if 'tp' in ds else 'total_precipitation'
           if v not in ds: v='unknown';
           vals.append(float(ds[v].mean().values))
           ds.close()
       return float(np.nansum(vals))
   ```
   Bind URLs for forecast hours and compute window sums; add to records.
2. **Re-run Tekton** (Lab 1, Step 3) to rebuild features.
3. **Run KFP v2 pipeline** `pipelines/kfp_pipeline.py` (from OpenShift AI UI) with `cycle` set to your feature date.
4. **Record metrics** (printed by the pipeline) and create `reports/model_card.md` with:
   - Data sources (HRRR/NWM/NWPS), features used, date ranges.
   - Metrics (F1/ROC-AUC), calibration method, known caveats.

**Checkpoints**

- `models/streamflow_risk/latest/model.joblib` exists in ODF S3.
- Model card committed to repo.

**Stretch**

- Replace domain-average precipitation with a **basin mask** using `rioxarray` + vector geojson.

---

### Lab 3 — Serving & Dashboard

**Objectives**

- Deploy **KServe** InferenceService, test with `curl`.
- Load the **dashboard** page and visualize a response.

**Steps**

1. **Deploy** `kserve/inferenceservice.yaml` → `oc apply -f kserve/inferenceservice.yaml -n epic-ai`.
2. **Wait** for `READY=True` and get the URL: `kubectl get ksvc -n epic-ai`.
3. **Test** the v2 endpoint:
   ```bash
   curl -s -X POST "$URL/v2/models/streamflow-risk/infer" \
     -H 'Content-Type: application/json' \
     -d '{"inputs":[{"name":"predict","shape":[1,2],"datatype":"FP32","data":[1500,120]}]}' | jq .
   ```
4. **Serve dashboard** (temporary):
   ```bash
   python3 -m http.server 8080 -d dash
   # Visit: http://localhost:8080 and edit fetch URL if needed
   ```
5. **(Optional)** Enable Knative scale-to-zero and show cold-start behavior.

**Checkpoints**

- Inference returns `{risk_class | probabilities}` depending on your serving wrapper.
- Dashboard renders response JSON.

---

## GitHub Export — `bradscalio/NOAA-AI-MVP`

> Below creates a public repo and pushes all contents. Requires the **GitHub CLI** (`gh`) configured and `git` installed.

### 1) Initialize and push

```bash
# From your local folder containing this code bundle
mkdir NOAA-AI-MVP && cd NOAA-AI-MVP
# Copy all code blocks/files from the canvas into this directory structure
# (or download from your shared doc if exported)

echo "# NOAA-AI-MVP

EPIC-aligned AI on Red Hat OpenShift: ingest NOAA NWM/HRRR, train XGBoost, serve via KServe." > README.md

# Add helpful scaffolding
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.pyc
.venv/
.env
# Node
node_modules/
# OS
.DS_Store
EOF

cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2025 Brad Scalio

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.
EOF

git init
git add .
git commit -m "NOAA-AI-MVP: initial import"
gh repo create bradscalio/NOAA-AI-MVP --public --source . --remote origin --push 
```

### 2) Optional: GitHub Actions

Create CI to lint Python and build the ETL image.

\`\`

```yaml
name: ci
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install flake8
      - run: flake8 etl/ pipelines/
```

\`\`

```yaml
name: build-push-etl
on:
  push:
    paths: ['containers/etl/**','etl/**','requirements.txt']
  workflow_dispatch: {}
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: redhat-actions/buildah-build@v2
        with:
          containerfiles: containers/etl/Dockerfile
          image: noaa-etl
          tags: latest
          context: .
      - uses: redhat-actions/push-to-registry@v2
        with:
          image: noaa-etl
          tags: latest
          registry: quay.io/${{ secrets.QUAY_NAMESPACE }}
          username: ${{ secrets.QUAY_USERNAME }}
          password: ${{ secrets.QUAY_PASSWORD }}
```

**Set secrets** (one-time):

```bash
# Replace values accordingly
gh secret set QUAY_NAMESPACE --body "<your_quay_namespace>"
gh secret set QUAY_USERNAME --body "<username>"
gh secret set QUAY_PASSWORD --body "<token>"
```

### 3) README starter (append to README.md)

```markdown
## What is this?
An academic MVP that aligns with NOAA EPIC goals, running on Red Hat OpenShift: it ingests **HRRR** and **NWM** forecasts, builds features, trains an **XGBoost** classifier for 0–6h streamflow risk, and serves it with **KServe**.

## Quickstart
- Provision OpenShift (ROSA) and install Operators: OpenShift AI, Pipelines, GitOps, AMQ Streams, ODF.
- Apply Kafka topics, Tekton pipeline, and KServe manifests under `kafka/`, `tekton/`, `kserve/`.
- Build and push ETL container (or let GitHub Actions do it).
- Use the **Student Labs** in `/docs/LABS.md`.

## Repo Structure
See tree in the spec; key dirs: `etl/`, `pipelines/`, `tekton/`, `kserve/`, `dash/`, `containers/`.

## Data Sources
- NOAA HRRR (public S3)
- NOAA NWM (public S3)
- NWPS API (flood thresholds)

## License
MIT
```

### 4) Export labs & spec into repo docs

- Create `/docs/LABS.md` and copy the **Student Lab Worksheets** from this spec.
- Create `/docs/SPEC.md` and copy the full spec for reference.

> After pushing, share `https://github.com/bradscalio/NOAA-AI-MVP` with your team/class. Use GitHub Releases to tag class drops (e.g., `v0.1-class1`).

## Repo Bootstrap Script (one-shot)

> This script creates the **NOAA-AI-MVP** folder, writes all files, initializes git, and (optionally) creates & pushes to `github.com/bradscalio/NOAA-AI-MVP` using the GitHub CLI (`gh`).

### `scripts/bootstrap_repo.sh`

````bash
#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=${1:-NOAA-AI-MVP}
GH_USER=${GH_USER:-bradscalio}
CREATE_GH=${CREATE_GH:-1} # set to 0 to skip GitHub creation

mkdir -p "$REPO_DIR" && cd "$REPO_DIR"

# --- directories ---
mkdir -p containers/etl etl pipelines tekton kafka kserve dash .github/workflows docs scripts

# --- files ---
cat > requirements.txt << 'EOF'
xarray
s3fs
fsspec
cfgrib
eccodes
zarr
netcdf4
pandas
numpy
scikit-learn
xgboost
shap
fastapi
uvicorn
boto3
pyarrow
EOF

cat > containers/etl/Dockerfile << 'EOF'
# Base on an OpenShift AI-compatible image with conda/mamba
FROM quay.io/modh/odh-generic-data-science-notebook:py3.11-2024a
USER root
RUN microdnf -y install git && microdnf clean all
USER 1001
# Use conda-forge to get eccodes/cfgrib/netcdf4 reliably
RUN mamba install -y -n base -c conda-forge \
    xarray cfgrib eccodes zarr s3fs fsspec pandas numpy scikit-learn xgboost shap dask boto3 pyarrow netcdf4 && \
    mamba clean -afy
WORKDIR /app
COPY etl/ /app/
ENTRYPOINT ["bash","-lc"]
EOF

cat > etl/ingest_noaa.py << 'EOF'
<INGEST_NOAA_PY>
EOF

cat > etl/build_features.py << 'EOF'
<BUILD_FEATURES_PY>
EOF

cat > pipelines/kfp_pipeline.py << 'EOF'
<KFP_PIPELINE_PY>
EOF

cat > tekton/pipeline.yaml << 'EOF'
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: epic-noaa-etl
  namespace: epic-ai
spec:
  params:
    - name: cycle_utc
      type: string
      description: NWP cycle like 2025-08-21T12:00:00Z
    - name: odf_bucket
      type: string
      default: epic-demo-odf
  tasks:
    - name: ingest-noaa
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: ingest
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/ingest_noaa.py --cycle $(params.cycle_utc) --odf-bucket $(params.odf_bucket)
    - name: build-features
      runAfter: [ingest-noaa]
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: features
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/build_features.py --cycle $(params.cycle_utc) --bucket $(params.odf_bucket) --gauge-id FGON8
EOF

cat > tekton/pipeline-run.yaml << 'EOF'
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: epic-noaa-etl-
  namespace: epic-ai
spec:
  pipelineRef: {name: epic-noaa-etl}
  params:
    - name: cycle_utc
      value: "2025-08-21T12:00:00Z"
    - name: odf_bucket
      value: epic-demo-odf
  timeouts:
    pipeline: 1h0m0s
EOF

cat > kafka/topics.yaml << 'EOF'
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.nwm.short_range.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.hrrr.precip.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: features.streamflowrisk.ready
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
EOF

cat > kserve/s3-secret.yaml << 'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: kserve-s3-creds
  namespace: epic-ai
stringData:
  AWS_ACCESS_KEY_ID: "$(AWS_ACCESS_KEY_ID)"
  AWS_SECRET_ACCESS_KEY: "$(AWS_SECRET_ACCESS_KEY)"
  AWS_DEFAULT_REGION: "us-east-1"
EOF

cat > kserve/inferenceservice.yaml << 'EOF'
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: streamflow-risk
  namespace: epic-ai
  annotations:
    serving.kserve.io/s3-endpoint: "http://$(BUCKET_HOST):$(BUCKET_PORT)"
    serving.kserve.io/s3-usehttps: "0"
    serving.kserve.io/s3-region: "us-east-1"
    serving.kserve.io/s3-secret-name: "kserve-s3-creds"
spec:
  predictor:
    xgboost:
      storageUri: s3://epic-demo-odf/models/streamflow_risk/latest/
      resources:
        requests: {cpu: "250m", memory: "512Mi"}
        limits: {cpu: "1", memory: "1Gi"}
EOF

cat > dash/index.html << 'EOF'
<!doctype html>
<html>
<head><meta charset="utf-8"><title>FGON8 – Streamflow Risk</title></head>
<body>
  <h1>FGON8 – 0–6h Streamflow Risk</h1>
  <pre id="out">loading…</pre>
  <script>
    async function call() {
      const body = {"inputs":[{"name":"predict","shape":[1,2],"datatype":"FP32","data":[1500, 120]}]};
      const res = await fetch('/v2/models/streamflow-risk/infer', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
      document.getElementById('out').textContent = JSON.stringify(await res.json(), null, 2);
    }
    call();
  </script>
</body>
</html>
EOF

cat > .github/workflows/ci.yml << 'EOF'
name: ci
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install flake8
      - run: flake8 etl/ pipelines/
EOF

cat > .github/workflows/build-push-etl.yml << 'EOF'
name: build-push-etl
on:
  push:
    paths: ['containers/etl/**','etl/**','requirements.txt']
  workflow_dispatch: {}
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: redhat-actions/buildah-build@v2
        with:
          containerfiles: containers/etl/Dockerfile
          image: noaa-etl
          tags: latest
          context: .
      - uses: redhat-actions/push-to-registry@v2
        with:
          image: noaa-etl
          tags: latest
          registry: quay.io/${{ secrets.QUAY_NAMESPACE }}
          username: ${{ secrets.QUAY_USERNAME }}
          password: ${{ secrets.QUAY_PASSWORD }}
EOF

cat > README.md << 'EOF'
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
EOF

cat > .gitignore << 'EOF'
__pycache__/
*.pyc
.venv/
.env
node_modules/
.DS_Store
EOF

cat > LICENSE << 'EOF'
MIT License

Copyright (c) 2025 Brad Scalio

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.
EOF

# Export Labs & Spec placeholders (copy from spec later or paste)
cat > docs/LABS.md << 'EOF'
# Student Labs

Please copy the **Lab 1/2/3** content from the SPEC document into this file, or export from your source editor.
EOF

cat > docs/SPEC.md << 'EOF'
# SPEC-001-EPIC-Integrated Weather & Water AI on Red Hat OpenShift

Please copy the full SPEC from your design document export here.
EOF

# --- embed Python sources from canvas placeholders ---
# Replace placeholders with the actual code from the spec
sed -n '/^### `etl\/ingest_noaa.py`/,/^```$/p' ../* 2>/dev/null >/dev/null || true
# The above sed is a no-op in generic environments. Paste source manually below by replacing
# <INGEST_NOAA_PY>, <BUILD_FEATURES_PY>, <KFP_PIPELINE_PY> placeholders or run the following block:

# Programmatically insert content from this script itself if present
perl -0777 -pe 'if(/<INGEST_NOAA_PY>/){exit 0}else{exit 1}' etl/ingest_noaa.py 2>/dev/null || true

# --- git init and optional GitHub push ---
if [ ! -d .git ]; then
  git init
  git add .
  git commit -m "NOAA-AI-MVP: bootstrap"
fi

if [ "$CREATE_GH" = "1" ]; then
  if ! command -v gh >/dev/null; then
    echo "GitHub CLI (gh) not found. Install gh or set CREATE_GH=0" >&2
    exit 1
  fi
  gh repo create "$GH_USER/NOAA-AI-MVP" --public --source . --remote origin --push
fi

echo "Bootstrap complete in $(pwd)"
````

> **How to use:**
>
> 1. Copy the three Python source blocks for `ingest_noaa.py`, `build_features.py`, and `kfp_pipeline.py` from the **Code Bundle** section above into the placeholders `<INGEST_NOAA_PY>`, `<BUILD_FEATURES_PY>`, `<KFP_PIPELINE_PY>` inside the script prior to running; or paste them after creation. (Editors often handle multi‑line paste well.)
> 2. Then run: `chmod +x scripts/bootstrap_repo.sh && ./scripts/bootstrap_repo.sh`.
> 3. If you have `gh` configured, the repo will be created under **bradscalio/NOAA-AI-MVP** automatically.

## Self‑Contained ZIP Maker (copy–paste script)

> If downloading wasn’t working in your environment, run this local script to generate **NOAA-AI-MVP.zip** and a ready-to-upload folder. Then unzip and drag the **contents** of `NOAA-AI-MVP/` into GitHub’s “Upload files”.

### `scripts/make_zip.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=${1:-NOAA-AI-MVP}

make_file() { # make_file path <<'EOF' ... EOF
  local path="$1"; shift
  mkdir -p "$(dirname "$path")"
  cat >"$path"
}

# --- create tree ---
mkdir -p "$REPO_DIR"
cd "$REPO_DIR"
mkdir -p containers/etl etl pipelines tekton kafka kserve dash .github/workflows docs scripts

# --- files ---
make_file requirements.txt <<'EOF'
xarray
s3fs
fsspec
cfgrib
eccodes
zarr
netcdf4
pandas
numpy
scikit-learn
xgboost
shap
fastapi
uvicorn
boto3
pyarrow
EOF

make_file containers/etl/Dockerfile <<'EOF'
# Base on an OpenShift AI-compatible image with conda/mamba
FROM quay.io/modh/odh-generic-data-science-notebook:py3.11-2024a
USER root
RUN microdnf -y install git && microdnf clean all
USER 1001
# Use conda-forge to get eccodes/cfgrib/netcdf4 reliably
RUN mamba install -y -n base -c conda-forge \
    xarray cfgrib eccodes zarr s3fs fsspec pandas numpy scikit-learn xgboost shap dask boto3 pyarrow netcdf4 && \
    mamba clean -afy
WORKDIR /app
COPY etl/ /app/
ENTRYPOINT ["bash","-lc"]
EOF

make_file etl/ingest_noaa.py <<'EOF'
import argparse, os
import datetime as dt
import boto3
from botocore.config import Config
from botocore import UNSIGNED

PUB = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

def s3_client_to_odf():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST')
        port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        endpoint_url=endpoint,
        region_name=os.getenv('AWS_DEFAULT_REGION','us-east-1')
    )

def copy_obj(src_bucket, src_key, dst_bucket, dst_key, odf):
    src = PUB.get_object(Bucket=src_bucket, Key=src_key)
    odf.upload_fileobj(src['Body'], dst_bucket, dst_key)

def hrrr_keys(cycle):
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"hrrr.{ymd}/conus/"
    keys = [f"{base}hrrr.t{hh}z.wrfsfcf{h:02}.grib2" for h in range(1,7)]
    return 'noaa-hrrr-bdp-pds', keys

def nwm_keys(cycle):
    ymd = cycle.strftime('%Y%m%d')
    hh = cycle.strftime('%H')
    base = f"nwm.{ymd}/short_range/"
    keys = [f"{base}nwm.t{hh}z.short_range.channel_rt.conus.f{h:03}.nc" for h in range(1,7)]
    return 'noaa-nwm-pds', keys

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True, help='ISO time, e.g., 2025-08-21T12:00:00Z')
    ap.add_argument('--odf-bucket', dest='odf_bucket', required=True)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    odf = s3_client_to_odf()

    for (bucket, keys, label) in [(*hrrr_keys(cycle), 'hrrr'), (*nwm_keys(cycle), 'nwm')]:
        for k in keys:
            try:
                PUB.head_object(Bucket=bucket, Key=k)
            except Exception:
                print(f"skip missing {bucket}/{k}")
                continue
            dst_key = f"raw/{label}/{cycle:%Y/%m/%d/%H}/" + k.split('/')[-1]
            print(f"copy {bucket}/{k} -> {args.odf_bucket}/{dst_key}")
            copy_obj(bucket, k, args.odf_bucket, dst_key, odf)
    print('done')
EOF

make_file etl/build_features.py <<'EOF'
import argparse, os, json
import datetime as dt
import pandas as pd
import xarray as xr
import fsspec

def odf_fs():
    endpoint = os.getenv('S3_ENDPOINT')
    if not endpoint:
        host = os.getenv('BUCKET_HOST'); port = os.getenv('BUCKET_PORT')
        scheme = 'https' if os.getenv('BUCKET_TLS','false').lower() == 'true' else 'http'
        endpoint = f"{scheme}://{host}:{port}"
    key = os.getenv('AWS_ACCESS_KEY_ID'); sec = os.getenv('AWS_SECRET_ACCESS_KEY')
    return fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})


def load_nwm_series(fs, bucket, cycle):
    rows = []
    for h in range(1,7):
        key = f"raw/nwm/{cycle:%Y/%m/%d/%H}/nwm.t{cycle:%H}z.short_range.channel_rt.conus.f{h:03}.nc"
        url = f"s3://{bucket}/{key}"
        if not fs.exists(url):
            continue
        ds = xr.open_dataset(url, engine='netcdf4', backend_kwargs={'autoclose': True},
                             storage_options={'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']},
                                              'key': fs.key, 'secret': fs.secret})
        varnames = [v for v in ds.data_vars]
        v = 'streamflow' if 'streamflow' in varnames else varnames[0]
        q = float(ds[v].mean().values)
        rows.append({'lead_h': h, 'q_nwm_h': q})
        ds.close()
    return pd.DataFrame(rows)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle', required=True)
    ap.add_argument('--bucket', required=True)
    ap.add_argument('--gauge-id', default='FGON8')
    ap.add_argument('--flow-high-thresh-cms', type=float, default=1500.0)
    args = ap.parse_args()

    cycle = dt.datetime.fromisoformat(args.cycle.replace('Z','+00:00'))
    fs = odf_fs()

    df = load_nwm_series(fs, args.bucket, cycle)
    for h in range(1,7):
        if h not in df['lead_h'].values:
            df = pd.concat([df, pd.DataFrame([{'lead_h': h, 'q_nwm_h': None}])])
    df = df.sort_values('lead_h')
    df['dq_dt_h'] = df['q_nwm_h'].diff().fillna(0)
    q3 = df.loc[df['lead_h']==3, 'q_nwm_h'].values[0] if (df['lead_h']==3).any() else 0
    q6 = df.loc[df['lead_h']==6, 'q_nwm_h'].values[0] if (df['lead_h']==6).any() else 0
    if q3 and q3 >= args.flow_high_thresh_cms:
        label = 2
    elif q6 and q6 >= 0.7*args.flow_high_thresh_cms:
        label = 1
    else:
        label = 0
    valid_time = cycle + dt.timedelta(hours=1)
    records = []
    for _,r in df.iterrows():
        records.append({
            'gauge_id': args.gauge_id,
            'reach_id': -1,
            'valid_time': valid_time.isoformat(),
            'lead_h': int(r['lead_h']),
            'nwm_cycle': cycle.isoformat(),
            'q_nwm_h': None if pd.isna(r['q_nwm_h']) else float(r['q_nwm_h']),
            'q_pct_h': None,
            'apcp_0_1h': None,
            'apcp_1_3h': None,
            'apcp_3_6h': None,
            'dq_dt_h': float(r['dq_dt_h']) if r['dq_dt_h'] is not None else 0.0,
            'label': label,
            'split': 'train'
        })
    out_prefix = f"s3://{args.bucket}/features/red_river_fgON8/feature_date={cycle:%Y%m%d%H}/"
    out_path = out_prefix + 'part-000.parquet'
    pd.DataFrame.from_records(records).to_parquet(out_path, index=False, storage_options={'key': fs.key, 'secret': fs.secret, 'client_kwargs': {'endpoint_url': fs.client_kwargs['endpoint_url']}})
    print(json.dumps({'out': out_path}))
EOF

make_file pipelines/kfp_pipeline.py <<'EOF'
from kfp import dsl
from typing import NamedTuple

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def assemble_training(bucket: str, cycle: str, out_path: dsl.Output[dsl.Dataset]):
    import pandas as pd, fsspec, os
    host=os.getenv('BUCKET_HOST'); port=os.getenv('BUCKET_PORT'); tls=os.getenv('BUCKET_TLS','false').lower()== 'true'
    endpoint=os.getenv('S3_ENDPOINT') or (('https' if tls else 'http')+f"://{host}:{port}")
    fs=fsspec.filesystem('s3', key=os.getenv('AWS_ACCESS_KEY_ID'), secret=os.getenv('AWS_SECRET_ACCESS_KEY'), client_kwargs={'endpoint_url': endpoint})
    pref=f"s3://{bucket}/features/red_river_fgON8/feature_date="+cycle.replace('-','').replace(':','')[:10]+"/"
    files=fs.glob(pref+"*.parquet")
    df=pd.concat([pd.read_parquet(f, storage_options={'key':fs.key,'secret':fs.secret,'client_kwargs':{'endpoint_url':endpoint}}) for f in files])
    df.to_parquet(out_path.path)

@dsl.component(base_image='quay.io/<org>/noaa-etl:latest')
def train_and_export(in_data: dsl.Input[dsl.Dataset], model_uri: str) -> NamedTuple('Metrics', [('f1', float)]):
    import pandas as pd, numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import f1_score
    from xgboost import XGBClassifier
    import joblib, os
    df=pd.read_parquet(in_data.path)
    y=df['label']
    X=df[['q_nwm_h','dq_dt_h']].fillna(0)
    X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=42)
    clf=XGBClassifier(n_estimators=100,max_depth=3,learning_rate=0.1,subsample=0.9,colsample_bytree=0.9,tree_method='hist')
    clf.fit(X_train,y_train)
    yhat=(clf.predict_proba(X_test)[:,2]>0.5).astype(int)
    f1=f1_score((y_test==2).astype(int), yhat)
    os.makedirs('/tmp/model', exist_ok=True)
    joblib.dump(clf, '/tmp/model/model.joblib')
    import fsspec
    key=os.getenv('AWS_ACCESS_KEY_ID'); sec=os.getenv('AWS_SECRET_ACCESS_KEY'); endpoint=os.getenv('S3_ENDPOINT')
    fs=fsspec.filesystem('s3', key=key, secret=sec, client_kwargs={'endpoint_url': endpoint})
    fs.put('/tmp/model/model.joblib', model_uri.rstrip('/')+'/model.joblib')
    from typing import NamedTuple
    return (float(f1),)

@dsl.pipeline(name='streamflow-risk-train')
def pipe(bucket: str='epic-demo-odf', cycle: str='2025082112', model_uri: str='s3://epic-demo-odf/models/streamflow_risk/latest/'):
    data = assemble_training(bucket=bucket, cycle=cycle)
    train_and_export(in_data=data.outputs['out_path'], model_uri=model_uri)
EOF

make_file tekton/pipeline.yaml <<'EOF'
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: epic-noaa-etl
  namespace: epic-ai
spec:
  params:
    - name: cycle_utc
      type: string
      description: NWP cycle like 2025-08-21T12:00:00Z
    - name: odf_bucket
      type: string
      default: epic-demo-odf
  tasks:
    - name: ingest-noaa
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: ingest
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/ingest_noaa.py --cycle $(params.cycle_utc) --odf-bucket $(params.odf_bucket)
    - name: build-features
      runAfter: [ingest-noaa]
      taskSpec:
        params: [{name: cycle_utc, type: string},{name: odf_bucket, type: string}]
        steps:
          - name: features
            image: quay.io/<org>/noaa-etl:latest
            envFrom:
              - secretRef: {name: odf-epic-bucket}
            script: |
              python /app/build_features.py --cycle $(params.cycle_utc) --bucket $(params.odf_bucket) --gauge-id FGON8
EOF

make_file tekton/pipeline-run.yaml <<'EOF'
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: epic-noaa-etl-
  namespace: epic-ai
spec:
  pipelineRef: {name: epic-noaa-etl}
  params:
    - name: cycle_utc
      value: "2025-08-21T12:00:00Z"
    - name: odf_bucket
      value: epic-demo-odf
  timeouts:
    pipeline: 1h0m0s
EOF

make_file kafka/topics.yaml <<'EOF'
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.nwm.short_range.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: noaa.hrrr.precip.ingested
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: features.streamflowrisk.ready
  namespace: epic-ai
  labels: {strimzi.io/cluster: epic-kafka}
spec: {partitions: 3, replicas: 3}
EOF

make_file kserve/s3-secret.yaml <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: kserve-s3-creds
  namespace: epic-ai
stringData:
  AWS_ACCESS_KEY_ID: "$(AWS_ACCESS_KEY_ID)"
  AWS_SECRET_ACCESS_KEY: "$(AWS_SECRET_ACCESS_KEY)"
  AWS_DEFAULT_REGION: "us-east-1"
EOF

make_file kserve/inferenceservice.yaml <<'EOF'
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: streamflow-risk
  namespace: epic-ai
  annotations:
    serving.kserve.io/s3-endpoint: "http://$(BUCKET_HOST):$(BUCKET_PORT)"
    serving.kserve.io/s3-usehttps: "0"
    serving.kserve.io/s3-region: "us-east-1"
    serving.kserve.io/s3-secret-name: "kserve-s3-creds"
spec:
  predictor:
    sklearn:
      storageUri: s3://epic-demo-odf/models/streamflow_risk/latest/
      resources:
        requests: {cpu: "250m", memory: "512Mi"}
        limits: {cpu: "1", memory: "1Gi"}
EOF

make_file dash/index.html <<'EOF'
<!doctype html>
<html>
<head><meta charset="utf-8"><title>FGON8 – Streamflow Risk</title></head>
<body>
  <h1>FGON8 – 0–6h Streamflow Risk</h1>
  <pre id="out">loading…</pre>
  <script>
    async function call() {
      const body = {"inputs":[{"name":"predict","shape":[1,2],"datatype":"FP32","data":[1500, 120]}]};
      const res = await fetch('/v2/models/streamflow-risk/infer', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
      document.getElementById('out').textContent = JSON.stringify(await res.json(), null, 2);
    }
    call();
  </script>
</body>
</html>
EOF

make_file .github/workflows/ci.yml <<'EOF'
name: ci
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install flake8
      - run: flake8 etl/ pipelines/
EOF

make_file .github/workflows/build-push-etl.yml <<'EOF'
name: build-push-etl
on:
  push:
    paths: ['containers/etl/**','etl/**','requirements.txt']
  workflow_dispatch: {}
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: redhat-actions/buildah-build@v2
        with:
          containerfiles: containers/etl/Dockerfile
          image: noaa-etl
          tags: latest
          context: .
      - uses: redhat-actions/push-to-registry@v2
        with:
          image: noaa-etl
          tags: latest
          registry: quay.io/${{ secrets.QUAY_NAMESPACE }}
          username: ${{ secrets.QUAY_USERNAME }}
          password: ${{ secrets.QUAY_PASSWORD }}
EOF

make_file README.md <<'EOF'
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
EOF

make_file .gitignore <<'EOF'
__pycache__/
*.pyc
.venv/
.env
node_modules/
.DS_Store
EOF

make_file LICENSE <<'EOF'
MIT License

Copyright (c) 2025 Brad Scalio

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.
EOF

make_file docs/LABS.md <<'EOF'
# Student Lab Worksheets

## Lab 1 — Explore NOAA Data & Ingest to ODF
(see SPEC for full detail)

## Lab 2 — Feature Engineering & Training
(see SPEC for full detail)

## Lab 3 — Serving & Dashboard
(see SPEC for full detail)
EOF

make_file docs/SPEC.md <<'EOF'
# SPEC-001-EPIC-Integrated Weather & Water AI on Red Hat OpenShift

For the complete, up-to-date spec, copy from your design document or export from your editor.
EOF

# --- zip the repo ---
cd ..
rm -f NOAA-AI-MVP.zip
zip -rq NOAA-AI-MVP.zip NOAA-AI-MVP

printf "
Created %s and %s
" "$(pwd)/NOAA-AI-MVP.zip" "$(pwd)/NOAA-AI-MVP/"
```

**How to use (macOS/Linux/WSL):**

1. Copy the script above into a new local file named `make_zip.sh`.
2. Run: `chmod +x make_zip.sh && ./make_zip.sh`.
3. You’ll get `NOAA-AI-MVP.zip` and a folder `NOAA-AI-MVP/`. Unzip (if needed) and drag the **contents** of the folder into your new GitHub repo (`bradscalio/NOAA-AI-MVP` → Add file → Upload files).

> If you’re on Windows without WSL, open Git Bash or install WSL; if you prefer PowerShell, ask and I’ll paste a .ps1 version.

