#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
ENV_EXAMPLE="${PROJECT_ROOT}/.env.example"
TFVARS_FILE="${PROJECT_ROOT}/infra/terraform.tfvars"
TFVARS_EXAMPLE="${PROJECT_ROOT}/infra/terraform.tfvars.example"
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}"

SKIP_TERRAFORM=false
SKIP_VALIDATE=false
AUTO_APPROVE=false
PROJECT_ID=""
REGION=""
BQ_LOCATION=""
ENVIRONMENT=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/set_dataset.sh <dataset_id> [options]

Fast first-time setup:
  ./scripts/set_dataset.sh shipping_data --project-id my-gcp-project --region europe-west1 --bq-location EU --environment prod

Dataset switch only:
  ./scripts/set_dataset.sh shipping_data --skip-terraform

Options:
  --project-id <value>
  --region <value>
  --bq-location <value>
  --environment <value>
  --skip-terraform
  --skip-validate
  --auto-approve
EOF
}

if [[ $# -ge 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

DATASET_ID="$1"
shift

if [[ ! "${DATASET_ID}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "Dataset id must match ^[A-Za-z_][A-Za-z0-9_]*$"
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id)
      PROJECT_ID="${2:-}"
      shift
      ;;
    --region)
      REGION="${2:-}"
      shift
      ;;
    --bq-location)
      BQ_LOCATION="${2:-}"
      shift
      ;;
    --environment)
      ENVIRONMENT="${2:-}"
      shift
      ;;
    --skip-terraform)
      SKIP_TERRAFORM=true
      ;;
    --skip-validate)
      SKIP_VALIDATE=true
      ;;
    --auto-approve)
      AUTO_APPROVE=true
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
  shift
done

upsert_env_var() {
  local key="$1"
  local value="$2"
  local file="$3"

  if grep -Eq "^${key}=" "${file}"; then
    sed -i "s|^${key}=.*$|${key}=${value}|" "${file}"
  else
    printf '%s=%s\n' "${key}" "${value}" >> "${file}"
  fi
}

remove_env_var() {
  local key="$1"
  local file="$2"

  if [[ -f "${file}" ]]; then
    sed -i "/^${key}=/d" "${file}"
  fi
}

upsert_tfvars_string() {
  local key="$1"
  local value="$2"
  local file="$3"
  local line="${key}  = \"${value}\""

  if grep -Eq "^${key}[[:space:]]*=" "${file}"; then
    sed -i "s|^${key}[[:space:]]*=.*$|${line}|" "${file}"
  else
    printf '%s\n' "${line}" >> "${file}"
  fi
}

remove_tfvars_key() {
  local key="$1"
  local file="$2"

  if [[ -f "${file}" ]]; then
    sed -i "/^${key}[[:space:]]*=/d" "${file}"
  fi
}

read_tfvars_value() {
  local key="$1"
  local file="$2"
  awk -F'"' -v target="${key}" '$1 ~ "^" target "[[:space:]]*=" { print $2; exit }' "${file}"
}

resolve_cli() {
  local base="$1"

  if command -v "${base}" >/dev/null 2>&1; then
    command -v "${base}"
    return 0
  fi

  if command -v "${base}.exe" >/dev/null 2>&1; then
    command -v "${base}.exe"
    return 0
  fi

  return 1
}

run_bruin() {
  local bruin_bin
  bruin_bin="$(resolve_cli bruin || true)"
  if [[ -z "${bruin_bin}" ]]; then
    echo "Could not find bruin or bruin.exe in this shell."
    echo "If you are running from PowerShell, use scripts/set_dataset.ps1 or rerun with --skip-validate."
    return 127
  fi

  "${bruin_bin}" "$@"
}

run_terraform() {
  local terraform_bin
  terraform_bin="$(resolve_cli terraform || true)"
  if [[ -z "${terraform_bin}" ]]; then
    echo "Could not find terraform or terraform.exe in this shell."
    echo "If you are running from PowerShell, use scripts/set_dataset.ps1 or rerun with --skip-terraform."
    return 127
  fi

  "${terraform_bin}" "$@"
}

run_sync_script() {
  if command -v uv >/dev/null 2>&1; then
    if (
      cd "${PROJECT_ROOT}" &&
      UV_CACHE_DIR="${UV_CACHE_DIR}" uv run --no-project python ./scripts/sync_bruin_dataset.py
    ); then
      return 0
    fi
    echo "uv run failed for the sync step, falling back to python3..."
  fi

  (
    cd "${PROJECT_ROOT}"
    python3 ./scripts/sync_bruin_dataset.py
  )
}

echo "Configuring dataset '${DATASET_ID}'..."

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ENV_EXAMPLE}" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.example"
fi

upsert_env_var "THALASSA_BQ_DATASET" "${DATASET_ID}" "${ENV_FILE}"
remove_env_var "THALASSA_INTELLIGENCE_TABLE" "${ENV_FILE}"
if [[ -n "${PROJECT_ID}" ]]; then
  upsert_env_var "THALASSA_BQ_PROJECT" "${PROJECT_ID}" "${ENV_FILE}"
fi
if [[ -n "${BQ_LOCATION}" ]]; then
  upsert_env_var "THALASSA_BQ_LOCATION" "${BQ_LOCATION}" "${ENV_FILE}"
fi

echo "Syncing Bruin asset prefixes..."
run_sync_script

if [[ "${SKIP_VALIDATE}" == "false" ]]; then
  echo "Validating pipeline..."
  (
    cd "${PROJECT_ROOT}"
    run_bruin validate ./pipeline --fast
  )
fi

if [[ "${SKIP_TERRAFORM}" == "true" ]]; then
  echo "Skipping Terraform."
  exit 0
fi

if [[ ! -f "${TFVARS_FILE}" ]]; then
  cp "${TFVARS_EXAMPLE}" "${TFVARS_FILE}"
  echo "Created ${TFVARS_FILE} from the example."
fi

remove_tfvars_key "dataset_id" "${TFVARS_FILE}"
if [[ -n "${PROJECT_ID}" ]]; then
  upsert_tfvars_string "project_id" "${PROJECT_ID}" "${TFVARS_FILE}"
fi
if [[ -n "${REGION}" ]]; then
  upsert_tfvars_string "region" "${REGION}" "${TFVARS_FILE}"
fi
if [[ -n "${BQ_LOCATION}" ]]; then
  upsert_tfvars_string "bq_location" "${BQ_LOCATION}" "${TFVARS_FILE}"
fi
if [[ -n "${ENVIRONMENT}" ]]; then
  upsert_tfvars_string "environment" "${ENVIRONMENT}" "${TFVARS_FILE}"
fi

TF_PROJECT_ID="$(read_tfvars_value "project_id" "${TFVARS_FILE}")"
TF_BQ_LOCATION="$(read_tfvars_value "bq_location" "${TFVARS_FILE}")"
TF_ENVIRONMENT="$(read_tfvars_value "environment" "${TFVARS_FILE}")"

if [[ -z "${TF_PROJECT_ID}" || "${TF_PROJECT_ID}" == "your-gcp-project-id" ]]; then
  echo "Missing real project_id. If you only want to switch the dataset locally, rerun with --skip-terraform."
  echo "Otherwise rerun with --project-id or edit infra/terraform.tfvars."
  exit 1
fi
if [[ -z "${TF_BQ_LOCATION}" ]]; then
  echo "Missing bq_location. If you only want to switch the dataset locally, rerun with --skip-terraform."
  echo "Otherwise rerun with --bq-location or edit infra/terraform.tfvars."
  exit 1
fi
if [[ -z "${TF_ENVIRONMENT}" ]]; then
  echo "Missing environment. If you only want to switch the dataset locally, rerun with --skip-terraform."
  echo "Otherwise rerun with --environment or edit infra/terraform.tfvars."
  exit 1
fi

echo "Running Terraform..."
(
  cd "${PROJECT_ROOT}"
  run_terraform -chdir=infra init
  run_terraform -chdir=infra plan -var "dataset_id=${DATASET_ID}"
  if [[ "${AUTO_APPROVE}" == "true" ]]; then
    run_terraform -chdir=infra apply -auto-approve -var "dataset_id=${DATASET_ID}"
  else
    run_terraform -chdir=infra apply -var "dataset_id=${DATASET_ID}"
  fi
)

echo "Done. Dataset is set to '${DATASET_ID}'."
