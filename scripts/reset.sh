#!/usr/bin/env bash
# scripts/reset.sh ENV_NAME
#
# Break-glass cleanup that runs Terraform directly, without Temporal. Prefer
# `python teardown.py ENV_NAME`; use this only when the worker or server is
# unavailable. Only works on the machine that ran the worker, since it relies
# on the initialized workspaces under terraform/_workspaces/ENV_NAME.
set -euo pipefail
ENV="${1:?usage: reset.sh <env>}"

cd "$(dirname "$0")/.."

for module in app compute network; do
  ws="terraform/_workspaces/$ENV/$module"
  if [ ! -d "$ws/.terraform" ]; then
    echo "$module: no initialized workspace, skipping"
    continue
  fi
  vars=(-var "env_name=$ENV")
  # Destroy doesn't use these values, but the variables are required.
  [ "$module" = "compute" ] && vars+=(-var "subnet_id=unused" -var "security_group_id=unused")
  (cd "$ws" && terraform destroy -auto-approve -input=false "${vars[@]}") || true
done

rm -rf "terraform/_workspaces/$ENV"
echo "reset complete: $ENV"
