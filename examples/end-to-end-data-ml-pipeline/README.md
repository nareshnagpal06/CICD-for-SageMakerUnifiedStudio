# Unified AI Operations: MLOps and DataOps with SMUS CLI

## Overview

This project provides a framework for deploying end-to-end data and ML pipelines to Amazon SageMaker Unified Studio using the [`aws-smus-cicd-cli`](https://github.com/aws/CICD-for-SageMakerUnifiedStudio). One manifest format. One CLI. One CI/CD pattern — whether you're ingesting raw data with Glue ETL or training an XGBoost model with SageMaker.

It includes two production-ready example pipelines that form a data lineage chain:

- **DataOps** — ingests, transforms, and validates bank marketing data using Glue and Athena
- **MLOps** — trains, evaluates, and deploys an XGBoost binary classifier using SageMaker Airflow operators

Both pipelines follow the same declarative workflow: define resources in YAML, deploy with one command, orchestrate on MWAA Serverless, and promote across environments (dev → test → prod) without code changes.

## Table of Contents

1. [Architecture](#architecture)
2. [How the SMUS CLI Deploys](#how-the-smus-cli-deploys)
3. [Pipelines](#pipelines)
4. [Prerequisites](#prerequisites)
5. [Quick Start](#quick-start)
6. [Deployment & Configuration](#deployment--configuration)
7. [CI/CD](#cicd)
8. [Infrastructure](#infrastructure)
9. [CLI Commands](#cli-commands)
10. [Project Structure](#project-structure)

## Architecture

The system consists of three pipelines that form a data lineage chain with event-driven deployment:

```mermaid
flowchart TD
    subgraph DataOps[DataOps Pipeline — Airflow]
        direction LR
        Ingest[Ingest Raw Data]:::input
        Transform[Transform + Quality]:::process
        CatalogTable[Glue Catalog<br/>bank_mktg_&lt;stage&gt;.campaign_results]:::success
        Ingest --> Transform --> CatalogTable
    end

    subgraph MLOps[MLOps Pipeline — Airflow]
        direction LR
        FeatureEng[Feature Engineering]:::process
        Train[Train XGBoost<br/>+ MLflow Tracking]:::hook
        Evaluate[Evaluate Model<br/>Batch Transform]:::process
        Register[Register Model<br/>PendingApproval]:::info
        FeatureEng --> Train --> Evaluate --> Register
    end

    subgraph Deploy[Deploy Pipeline — Event-Driven]
        direction LR
        Approve[Approve Model]:::input
        EB[EventBridge]:::process
        Lambda[Lambda]:::hook
        GHA[GitHub Actions]:::info
        DAG[deploy_pipeline]:::process
        EP[Prediction Endpoint<br/>bank-mktg-prediction-&lt;stage&gt;]:::alert
        Approve --> EB --> Lambda --> GHA --> DAG --> EP
    end

    DataOps -->|from_catalog| MLOps
    MLOps -->|model_registry| Deploy

    subgraph Stages[Environment Promotion — same manifest, no code changes]
        direction LR
        Dev[dev<br/>bank_mktg_dev]:::input
        Test[test<br/>bank_mktg_test]:::process
        Prod[prod<br/>bank_mktg_prod]:::alert
        Dev -->|promote| Test -->|promote| Prod
    end

    Deploy -.->|deployed per stage| Stages

    classDef input fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#1a1a1a
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#1a1a1a
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1a1a1a
    classDef info fill:#e0f2f1,stroke:#00695c,stroke-width:2px,color:#1a1a1a
    classDef hook fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px,color:#1a1a1a
    classDef alert fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#1a1a1a

    style DataOps fill:transparent,stroke:#01579b,stroke-width:2px
    style MLOps fill:transparent,stroke:#7b1fa2,stroke-width:2px
    style Deploy fill:transparent,stroke:#2e7d32,stroke-width:2px
    style Stages fill:transparent,stroke:#f9a825,stroke-width:2px
```

The coupling points:

- **DataOps → MLOps:** Glue Data Catalog. DataOps writes to `bank_mktg_<stage>.campaign_results`, MLOps reads from it.
- **MLOps → Deploy:** SageMaker Model Registry. Training registers models as `PendingManualApproval`. Every approval triggers deployment via EventBridge → Lambda → GitHub Actions → deploy_pipeline DAG.

Stage-prefixed names (`bank_mktg_dev`, `bank-mktg-prediction-dev`) ensure complete namespace isolation across environments.

### Deploy trigger behavior

The EventBridge rule is intentionally permissive — it matches every `Model Package State Change` event where `ModelApprovalStatus=Approved`, and the Lambda decides whether to dispatch:

- **Every genuine approval triggers the pipeline exactly once**, whether the model is approved from the SageMaker console, the SMUS UI, or the API. The Lambda keys off `UpdatedModelPackageFields` (it dispatches when `ModelApprovalStatus` is among the changed fields), so it does not depend on `previousModelApprovalStatus`, which API- and UI-driven approvals omit.
- **No infinite loop.** After each deploy, the promote workflow stamps `CustomerMetadataProperties` on the model version, which re-emits an `Approved` event. Those re-emits change only `CustomerMetadataProperties`, so the Lambda skips them instead of kicking off another deploy.

## How the SMUS CLI Deploys

```mermaid
flowchart TD
    subgraph LocalRepo[Repository]
        manifest[manifest.yaml]:::input
        workflows[workflows/*.yaml]:::input
        src[src/*.py]:::input
        data[data/*.csv]:::input
    end

    manifest --> CLI
    workflows --> CLI
    src --> CLI
    data --> CLI

    subgraph CLI[SMUS CLI]
        direction LR
        Describe[aws-smus-cicd-cli describe]:::process
        DeployCLI[aws-smus-cicd-cli deploy]:::process
        RunCLI[aws-smus-cicd-cli run]:::process
        Monitor[aws-smus-cicd-cli monitor]:::process
        Describe --> DeployCLI --> RunCLI --> Monitor
    end

    CLI --> S3
    CLI --> Studio
    CLI --> MWAA

    subgraph AWSServices[AWS Services]
        S3[Amazon S3<br/>Data, Artifacts,<br/>Models, Scripts]:::success
        Studio[SageMaker Unified Studio<br/>Domain, Project]:::info
        MWAA[MWAA Serverless<br/>Airflow DAGs,<br/>Scheduling]:::warning
    end

    S3 --> Execution
    MWAA --> Execution

    subgraph Execution[Workflow Execution]
        MLOps[MLOps Pipelines]:::process
        DataOps[DataOps Pipelines]:::alert
    end

    classDef input fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#1a1a1a
    classDef success fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1a1a1a
    classDef warning fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#1a1a1a
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#1a1a1a
    classDef info fill:#e0f2f1,stroke:#00695c,stroke-width:2px,color:#1a1a1a
    classDef hook fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px,color:#1a1a1a
    classDef alert fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#1a1a1a

    style LocalRepo fill:transparent,stroke:#01579b,stroke-width:2px
    style CLI fill:transparent,stroke:#7b1fa2,stroke-width:2px
    style AWSServices fill:transparent,stroke:#2e7d32,stroke-width:2px
    style Execution fill:transparent,stroke:#7b1fa2,stroke-width:2px
```

The SMUS CLI handles resource provisioning in dependency order, stage-specific configuration substitution, and the full deployment lifecycle. In CI, GitHub Actions drives these same commands — see [CI/CD](#cicd) for the OIDC and multi-stage setup.

## Pipelines

| Pipeline | Directory | Description |
| -------- | --------- | ----------- |
| **DataOps** | [`examples/dataops-pipeline/`](examples/dataops-pipeline/) | Glue ETL + Athena catalog registration |
| **MLOps Training** | [`examples/mlops-pipeline/`](examples/mlops-pipeline/) | Feature engineering, SageMaker training, evaluation, model registry |
| **Deploy (Event-Driven)** | [`examples/mlops-pipeline/workflows/deploy_pipeline.yaml`](examples/mlops-pipeline/workflows/deploy_pipeline.yaml) | EventBridge → Lambda → GitHub Actions → deploy_pipeline DAG → endpoint |

The MLOps pipeline depends on DataOps — run DataOps first to create the `campaign_results` table.

## Prerequisites

| Tool | Version | Purpose |
| ---- | ------- | ------- |
| Python | 3.11+ | Runtime for CLI and Glue scripts |
| AWS CLI | v2 | AWS resource management |
| `aws-smus-cicd-cli` | latest | Pipeline deployment and orchestration |
| `jq` | any | JSON parsing for shell scripts |

You also need:

- An AWS account with permissions for SageMaker, Glue, Athena, S3, IAM, and MWAA
- A SageMaker Unified Studio domain and project with MWAA Serverless enabled
- Environment variables set for every stage the manifest defines

These are the same values configured as GitHub variables/secrets for CI (the source of truth — see [GitHub configuration](#github-configuration-ci-source-of-truth)). For local runs, export them for each stage your manifest defines:

```bash
pip install aws-smus-cicd-cli

# Account ID (aws sts get-caller-identity) and the domain (resolved by region +
# the manifest's `purpose` tag) are detected automatically — do not export them.
# Only DEV_DOMAIN_REGION is strictly required; the rest have manifest defaults.

# DataOps (dev stage only):
export DEV_DOMAIN_REGION=<your-region>               # required
export DEV_PROJECT_NAME=<your-dev-project>           # optional (default: e2e-data-ml-ops-dev)
export DOMAIN_TAG_PURPOSE=<your-domain-purpose-tag>  # optional (default: smus-cicd-testing)

# MLOps also deploys test/prod, so additionally export (all optional, have defaults):
export TEST_DOMAIN_REGION=<your-region>
export TEST_PROJECT_NAME=<your-test-project>
export PROD_DOMAIN_REGION=<your-region>
export PROD_PROJECT_NAME=<your-prod-project>
export MLFLOW_TRACKING_SERVER_NAME=<your-mlflow-server-name>
```

Per-pipeline walkthroughs live in each sub-README (linked under [Pipelines](#pipelines)).

## Quick Start

A minimal local run of the DataOps pipeline against `dev`:

```bash
cd examples/dataops-pipeline
aws-smus-cicd-cli describe --manifest manifest.yaml --targets dev --connect
aws-smus-cicd-cli deploy --manifest manifest.yaml --targets dev
aws-smus-cicd-cli run --manifest manifest.yaml --targets dev --workflow data_pipeline
aws-smus-cicd-cli monitor --manifest manifest.yaml --targets dev --live
```

For the full CI-driven setup and the dev → test → prod promotion, follow [Deployment & Configuration](#deployment--configuration).

## Deployment & Configuration

End-to-end path from an empty repo to an automated dev → test → prod promotion. Steps 1–4 are one-time setup; steps 5–9 are the recurring deploy/promote flow.

### 1. Install prerequisites and set environment variables

Install the CLI and export the stage variables listed in [Prerequisites](#prerequisites):

```bash
pip install aws-smus-cicd-cli
aws sts get-caller-identity        # confirm your AWS credentials/account
```

### 2. Configure GitHub for CI/CD

All jobs run in a single GitHub Environment named `dev-aws-account`, which holds the OIDC role secret and region. Add them there:

```bash
REPO=<owner>/<repo>

# Single OIDC role secret (assumed directly by every job — single-hop, single-account)
gh secret set AWS_ROLE_ARN_DEV --repo "$REPO" --env dev-aws-account --body "arn:aws:iam::<acct>:role/<dev-oidc-role>"

# Region (feeds every stage's *_DOMAIN_REGION); everything else has manifest defaults
gh variable set DOMAIN_REGION --repo "$REPO" --env dev-aws-account --body "us-east-1"

# Optional: promote-gate approvers (comma/newline-separated; defaults to one user)
gh variable set MLOPS_APPROVERS --repo "$REPO" --body "user1,user2"
```

`AWS_ACCOUNT_ID` and the domain are resolved at runtime, so they don't need to be set. See [GitHub configuration](#github-configuration-ci-source-of-truth) for the full list.

### 3. Enable Issues (required for the promote approval gates)

The promote workflow's `approve-test` / `approve-prod` gates open a tracking **issue** and wait for an approver. Enable Issues once (needs repo admin):

```bash
gh api -X PATCH repos/<owner>/<repo> -f has_issues=true
```

### 4. Provision the OIDC provider and IAM role

```bash
cd examples/end-to-end-data-ml-pipeline
./scripts/setup-github-oidc.sh     # GitHub OIDC provider + IAM role for CI/CD
```

Because every stage uses the same OIDC role (`AWS_ROLE_ARN_DEV`), that role must be a **member/owner** of each SMUS project it deploys to (`e2e-data-ml-ops-dev`, `-test`, `-prod`). The deploying role automatically becomes the owner of any project it creates; for pre-existing projects, add the role as a member so it can list connections and deploy. (If it is only a member of the dev project, only `dev` deploys succeed.)

### 5. Deploy the DataOps pipeline (dev)

DataOps must run first — it creates the `campaign_results` table the MLOps pipeline reads. Push to `main` (path-filtered) or trigger manually:

```bash
gh workflow run e2e-dataops-pipeline.yml --ref main
```

### 6. Deploy the MLOps training pipeline

Deploys the training/deploy DAGs and provisions the event-driven deploy trigger in dev (checkbox `setup_infra`, default on). First store the GitHub token the trigger Lambda uses (see [Setting up the event-driven deploy trigger](#setting-up-the-event-driven-deploy-trigger)), then:

```bash
gh workflow run e2e-mlops-pipeline.yml --ref main -f stages=all -f setup_infra=true
```

### 7. Train and register a model

The deployed training DAG runs on schedule (or trigger it) in dev's project, trains the XGBoost model, and registers it in the `bank-mktg-prediction-models` registry as `PendingManualApproval`.

### 8. Approve the model → automatic promote cascade

Approve the model version in the SageMaker Model Registry (console, SMUS UI, or API). That fires **EventBridge → Lambda → `repository_dispatch` → `e2e-mlops-promote.yml`**, which runs `prepare → deploy-dev → approve-test → deploy-test → approve-prod → deploy-prod`. Respond `approved` on each approval issue to advance. See [Deploy trigger behavior](#deploy-trigger-behavior).

### 9. Monitor and validate

```bash
gh run list  --workflow=e2e-mlops-promote.yml
gh run view <run-id>
```

Each deploy job validates the target, runs the DAG, verifies the SageMaker processing job, runs an endpoint smoke test, and uploads deploy logs as an artifact.

## CI/CD

These GitHub Actions workflows (at the repository root) are this example's own end-to-end CI/CD — they exist to exercise the pipelines and demonstrate the deploy/promote pattern in practice, not as workflows customers author or edit. They run the single-account deploy for this example:

| Workflow | File | Purpose |
| -------- | ---- | ------- |
| DataOps | [`e2e-dataops-pipeline.yml`](../../.github/workflows/e2e-dataops-pipeline.yml) | Deploy and run the data pipeline |
| MLOps Training | [`e2e-mlops-pipeline.yml`](../../.github/workflows/e2e-mlops-pipeline.yml) | Deploy training pipeline + provision MLOps infra (dev) |
| MLOps Promote | [`e2e-mlops-promote.yml`](../../.github/workflows/e2e-mlops-promote.yml) | Event-driven dev → test → prod promote cascade on model approval |

CI/CD uses OIDC authentication (no long-lived credentials). Every job runs in the single `dev-aws-account` environment and assumes one OIDC role (`AWS_ROLE_ARN_DEV`) directly — single-hop, single-account, no separate deployment role. The DataOps and MLOps deploy jobs call the shared [`smus-direct-deploy.yml`](../../.github/workflows/smus-direct-deploy.yml) reusable, mapping `AWS_ROLE_ARN_DEV` into its generic `AWS_ROLE_ARN` secret and passing `environment_name: dev-aws-account`; stages differ only by the manifest target (project + region). The MLOps training workflow provisions the EventBridge + Lambda deploy trigger in dev only — model approval happens in dev's registry and drives the promote cascade across stages.

The promote workflow's stage gates (`approve-test`, `approve-prod`) use the [`trstringer/manual-approval`](https://github.com/trstringer/manual-approval) action, which opens a tracking **issue** and waits for a listed approver to comment `approved` (or `approve`/`lgtm`/`yes`). This requires **Issues enabled** on the repository and the workflow's `issues: write` permission. Approvers come from the `MLOPS_APPROVERS` variable (comma/newline-separated usernames); if unset it falls back to a single default approver.

### GitHub configuration (CI source of truth)

The workflows read their configuration from GitHub Actions **variables** and **secrets** — there is no config file checked into the repo. The OIDC role secret (`AWS_ROLE_ARN_DEV`) and `DOMAIN_REGION` live in the `dev-aws-account` environment.

Some values are derived at runtime and do not need to be set:

- **`AWS_ACCOUNT_ID`** — resolved via `aws sts get-caller-identity`.
- **Domain** — resolved by region + the `purpose` tag on the manifest's domain block (default `smus-cicd-testing`), so no domain *name* variable is needed.
- **Project owner** — the deploying principal is already the project owner, so the manifests no longer hardcode an owner role.

**Secrets** (stored in the `dev-aws-account` environment):

| Secret | Purpose |
| ------ | ------- |
| `AWS_ROLE_ARN_DEV` | Single OIDC role assumed by every job (all stages); mapped into the reusable's generic `AWS_ROLE_ARN` |

**Variables:**

| Variable | Scope | Purpose |
| -------- | ----- | ------- |
| `DOMAIN_REGION` | environment (`dev-aws-account`) | Region for all stages (feeds `*_DOMAIN_REGION`) |
| `DEV_PROJECT_NAME` / `TEST_PROJECT_NAME` / `PROD_PROJECT_NAME` | repo | SMUS project per stage (optional; manifest and workflows default to `e2e-data-ml-ops-{dev,test,prod}`) |
| `MLOPS_APPROVERS` | repo | Promote-gate approver list, comma/newline-separated (promote workflow; requires Issues enabled) |
| `MLFLOW_TRACKING_SERVER_NAME` | repo/environment | MLflow tracking server name (optional; manifest has a default) |
| `DOMAIN_TAG_PURPOSE` | repo/environment | Optional override for the domain `purpose` tag (defaults to `smus-cicd-testing`) |

## Infrastructure

| File | Purpose |
| ---- | ------- |
| [`scripts/setup-mlops-infra.sh`](scripts/setup-mlops-infra.sh) | Event-driven deploy trigger (Lambda + EventBridge rule + IAM role) |
| [`scripts/setup-github-oidc.sh`](scripts/setup-github-oidc.sh) | GitHub OIDC provider + IAM role for CI/CD |

### Setting up the event-driven deploy trigger

`setup-mlops-infra.sh` provisions the approval → deploy trigger (Lambda + EventBridge rule + IAM role) that fires the promote pipeline whenever a model is approved in the registry.

> In CI/CD this runs automatically — the MLOps training workflow provisions the trigger in `dev` (`setup_infra: true`), so only the Secrets Manager token (step 1) must exist beforehand. The steps below are for provisioning it manually, outside CI.

**1. Store a GitHub personal access token in Secrets Manager** (required for both CI and manual setup). The Lambda reads this token to send a `repository_dispatch` event to GitHub Actions. The token needs `repo` scope (or `contents: write` for a fine-grained token on the target repo).

```bash
aws secretsmanager create-secret \
  --name bank-mktg/github-token \
  --secret-string 'ghp_your_token_here'
```

**2. Point the trigger at your GitHub repository.** The Lambda dispatches to the repo named in the `GITHUB_REPO` environment variable (`owner/repo` format). Export it before running the setup script:

```bash
export GITHUB_REPO=<owner>/<repo>
```

**3. Run the setup script** for the target environment. Arguments are `<dev|test|prod> <account-id> <region> <project-name>`:

```bash
cd examples/end-to-end-data-ml-pipeline
./scripts/setup-mlops-infra.sh dev "$AWS_ACCOUNT_ID" "$DEV_DOMAIN_REGION" "$DEV_PROJECT_NAME"
```

The script is idempotent — re-running it updates the existing Lambda code and configuration. **Re-run it after any change to the trigger logic** (it calls `update-function-code`), since the deployed Lambda does not update automatically. In CI this happens on every MLOps training run.

Once provisioned, the rule is `ENABLED` immediately. Approving a model version in `bank-mktg-prediction-models` then triggers the dev → test → prod promote cascade through GitHub Actions. See [Deploy trigger behavior](#deploy-trigger-behavior) for how the trigger handles approvals and avoids re-deploy loops.

## CLI Commands

The `aws-smus-cicd-cli` provides the following commands for managing the pipeline lifecycle. See the [CLI Commands Reference](../../docs/cli-commands.md) for full options and examples.

| Command | Purpose |
| ------- | ------- |
| `create` | Create a new bundle manifest |
| `describe` | Validate and show bundle configuration (use `--connect` to pull live AWS info) |
| `bundle` | Package workflow and storage files from a source environment |
| `deploy` | Deploy a bundle to a target environment (auto-initializes if needed) |
| `run` | Trigger a workflow or run an Airflow CLI command |
| `logs` | Fetch workflow logs from CloudWatch |
| `monitor` | Monitor workflow status (use `--live` to poll until complete) |
| `test` | Run tests for pipeline targets |
| `integrate` | Integrate with external tools (e.g. Q CLI MCP server) |
| `destroy` | Delete all resources deployed by the manifest |

## Project Structure

```text
├── examples/
│   ├── dataops-pipeline/              # DataOps: Glue ETL + Athena
│   │   ├── manifest.yaml
│   │   ├── data/bank-mktg-sample.csv
│   │   ├── workflows/data_pipeline.yaml
│   │   └── src/
│   │       ├── glue-jobs/*.py
│   │       └── notebooks/validate_dataops.ipynb
│   └── mlops-pipeline/                # MLOps: SageMaker + MLflow
│       ├── manifest.yaml
│       ├── workflows/
│       │   ├── training_pipeline.yaml
│       │   └── deploy_pipeline.yaml
│       └── src/
│           ├── train_xgboost.py
│           ├── feature_engineering.py
│           ├── evaluate_model.py
│           ├── deploy_model.py
│           ├── requirements.txt
│           └── notebooks/
│               ├── evaluate_model.ipynb
│               └── validate_mlops.ipynb
└── scripts/                           # Setup and helper scripts
    ├── setup-mlops-infra.sh           # EventBridge + Lambda deploy trigger
    ├── setup-github-oidc.sh           # GitHub OIDC provider + IAM role
    ├── build-mlops-sourcedir.sh       # Build training sourcedir.tar.gz
    ├── mlops_helper.py                # Deploy status / smoke-test helpers
    └── test-deploy-trigger-event.json # Sample EventBridge event for testing
```

CI/CD workflows live at the repository root under [`.github/workflows/`](../../.github/workflows/) (see [CI/CD](#cicd)).
