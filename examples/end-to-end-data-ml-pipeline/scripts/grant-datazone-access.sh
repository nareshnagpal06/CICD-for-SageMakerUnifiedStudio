#!/usr/bin/env bash
# =============================================================================
# Grant a deploy role DataZone access across a whole domain
# =============================================================================
# Fixes the deploy-time failure:
#
#   AccessDeniedException when calling ListConnections:
#   User is not permitted to perform operation: ListConnections.
#   ❌ No S3 URI found for connection unknown (type: unknown)
#   📊 Total files deployed: 0
#
# Cause: the deploy role's existing DataZone permissions are scoped to a
# single project's resources (e.g. the dev project), so the same role can
# ListConnections on dev but is denied on a newly added project (test/prod)
# even when it is a project OWNER. Being a project owner is NOT sufficient on
# its own — the IAM identity also needs `datazone:*` permission on the target
# project/connection resources.
#
# This script attaches an ADDITIVE inline IAM policy that grants the DataZone
# actions the smus-cli deploy needs, scoped to the entire DOMAIN (so every
# project in the domain — dev, test, prod — is covered). IAM policy evaluation
# is additive for Allow statements, so this broadens access without touching
# the role's existing policies.
#
# It is idempotent (re-running just overwrites the same inline policy) and
# read/least-privilege oriented: it only grants the specific DataZone actions
# the deploy flow uses, not `datazone:*`.
#
# Usage:
#   ./scripts/grant-datazone-access.sh <role-name> <account-id> <region> <domain-id>
#
# Example:
#   ./scripts/grant-datazone-access.sh \
#     GitHubActionsOIDCRole 688930149168 us-east-1 dzd-crvisl9kqmyrqf
# =============================================================================
set -euo pipefail
export AWS_PAGER=""

ROLE_NAME="${1:?Usage: $0 <role-name> <account-id> <region> <domain-id>}"
ACCOUNT_ID="${2:?Usage: $0 <role-name> <account-id> <region> <domain-id>}"
REGION="${3:?Usage: $0 <role-name> <account-id> <region> <domain-id>}"
DOMAIN_ID="${4:?Usage: $0 <role-name> <account-id> <region> <domain-id>}"

POLICY_NAME="SmusDataZoneDomainDeployAccess"
# Use a wildcard under `domain/` rather than the exact domain ARN. DataZone
# authorizes connection actions (ListConnections/GetConnection) against
# sub-resource ARNs such as `…:domain/<domainId>/connection/<id>`, which an
# exact `…:domain/<domainId>` ARN does NOT match. In IAM, `*` matches `/`, so
# `domain/*` covers the domain and all of its sub-resources.
DOMAIN_ARN="arn:aws:datazone:${REGION}:${ACCOUNT_ID}:domain/*"

echo "=== Granting DataZone domain access ==="
echo "  Role      : ${ROLE_NAME}"
echo "  Account   : ${ACCOUNT_ID}"
echo "  Region    : ${REGION}"
echo "  Domain    : ${DOMAIN_ID}"
echo "  Policy    : ${POLICY_NAME} (inline, additive)"
echo "  Resource  : ${DOMAIN_ARN}"

# ── Verify account ───────────────────────────────────────────────────────────
CURRENT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
if [ "$CURRENT_ACCOUNT" != "$ACCOUNT_ID" ]; then
  echo "ERROR: Authenticated to $CURRENT_ACCOUNT, expected $ACCOUNT_ID" >&2
  exit 1
fi

# ── Verify the role exists ───────────────────────────────────────────────────
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "ERROR: IAM role '$ROLE_NAME' not found in account $ACCOUNT_ID" >&2
  exit 1
fi

# ── Attach the additive inline policy ────────────────────────────────────────
# Scoped to the domain ARN so ListConnections/GetConnection (and the other
# read + connect actions the deploy uses) succeed for every project in the
# domain, not just the one the role's existing policy is scoped to.
POLICY_DOCUMENT=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SmusDataZoneDomainRead",
      "Effect": "Allow",
      "Action": [
        "datazone:GetDomain",
        "datazone:GetProject",
        "datazone:ListProjects",
        "datazone:ListProjectMemberships",
        "datazone:ListConnections",
        "datazone:GetConnection",
        "datazone:ListEnvironments",
        "datazone:GetEnvironment"
      ],
      "Resource": "${DOMAIN_ARN}"
    }
  ]
}
EOF
)

echo "Applying inline policy '${POLICY_NAME}'..."
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document "$POLICY_DOCUMENT"

echo ""
echo "=== Done ==="
echo "  Granted read/connect DataZone access on domain ${DOMAIN_ID} to ${ROLE_NAME}."
echo "  Re-run the deploy — 'ListConnections' should now succeed for all projects"
echo "  in the domain (dev/test/prod)."
echo ""
echo "Verify with:"
echo "  aws iam get-role-policy --role-name ${ROLE_NAME} --policy-name ${POLICY_NAME}"
