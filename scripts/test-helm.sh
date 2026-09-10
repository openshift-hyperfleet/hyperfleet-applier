#!/usr/bin/env bash
set -euo pipefail

CHART_DIR="${CHART_DIR:-charts}"
RELEASE_NAME="${RELEASE_NAME:-test-release}"

HELM="${HELM:-helm}"
KUBECONFORM="${KUBECONFORM:-kubeconform}"

KUBECONFORM_FLAGS=(
  -strict
  -kubernetes-version 1.30.0
  -schema-location default
)

DEFAULT_SETS=(
  --set image.registry=quay.io
  --set image.repository=openshift-hyperfleet/hyperfleet-applier
  --set image.tag=test
  --set applier.managementCluster=test-cluster
  --set applier.pollInterval=5s
  --set redis.address=redis:6379
)

PASSED=0
FAILED=0
SCENARIO_FAILED=0

render() {
  "$HELM" template "$RELEASE_NAME" "$CHART_DIR" "${DEFAULT_SETS[@]}" "$@"
}

kubeconform_validate() {
  # KUBECONFORM may be supplied by the Makefile as:
  #   go tool -modfile=tools/go.mod kubeconform
  # shellcheck disable=SC2086
  $KUBECONFORM "${KUBECONFORM_FLAGS[@]}" "$@"
}

run_test() {
  SCENARIO_FAILED=0
  echo ""
  echo "Testing $1..."
}

pass() {
  if [ "$SCENARIO_FAILED" -eq 0 ]; then
    echo "  ✓ $1"
    PASSED=$((PASSED + 1))
  fi
}

fail() {
  echo "  ✗ FAIL: $1"
  FAILED=$((FAILED + 1))
  SCENARIO_FAILED=1
}

assert_contains() {
  local input="$1"
  local pattern="$2"
  local message="$3"

  if ! grep -Fq -- "$pattern" <<< "$input"; then
    fail "$message"
  fi
}

assert_not_contains() {
  local input="$1"
  local pattern="$2"
  local message="$3"

  if grep -Fq -- "$pattern" <<< "$input"; then
    fail "$message"
  fi
}

# ─── Pre-flight ───────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Testing Helm charts..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if ! command -v "$HELM" > /dev/null; then
  echo "ERROR: helm not found. Please install Helm:"
  echo "  brew install helm  # macOS"
  echo "  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash  # Linux"
  exit 1
fi

# ─── Lint ─────────────────────────────────────────────────────────────
#
# The default RBAC allowlist is intentionally empty, so lint with the
# explicit development wildcard enabled.

echo ""
echo "Linting Helm chart..."
"$HELM" lint "$CHART_DIR" "${DEFAULT_SETS[@]}" \
  --set rbac.devModeWildcard=true

# ─── Positive rendering tests ─────────────────────────────────────────

run_test "template with explicit RBAC allowlist"

OUTPUT=$(render \
  --set-json 'rbac.allowlist=[{"apiGroups":[""],"resources":["configmaps"]},{"apiGroups":["apps"],"resources":["deployments"]}]')

assert_contains "$OUTPUT" 'kind: ClusterRole' \
  "ClusterRole not found in rendered output"
assert_contains "$OUTPUT" 'configmaps' \
  "allowlisted configmaps resource not found in rendered output"
assert_contains "$OUTPUT" 'apps' \
  "allowlisted apps API group not found in rendered output"
assert_contains "$OUTPUT" 'deployments' \
  "allowlisted deployments resource not found in rendered output"

echo "$OUTPUT" | kubeconform_validate
pass "Explicit RBAC allowlist template"

run_test "template with devModeWildcard"

OUTPUT=$(render --set rbac.devModeWildcard=true)

assert_contains "$OUTPUT" 'kind: ClusterRole' \
  "ClusterRole not found in rendered output"
assert_contains "$OUTPUT" '*' \
  "wildcard RBAC permissions not found in rendered output"

WILDCARD_COUNT=$( (grep -Fo -- '*' <<< "$OUTPUT" || true) | wc -l | tr -d ' ' )
if [ "$WILDCARD_COUNT" -lt 2 ]; then
  fail "expected wildcard apiGroups and resources in rendered output"
fi

echo "$OUTPUT" | kubeconform_validate
pass "devModeWildcard template"

# ─── Validation failure tests ─────────────────────────────────────────

run_test "empty RBAC allowlist is rejected"

if OUTPUT=$(render --set-json 'rbac.allowlist=[]' 2>&1); then
  fail "expected helm template to fail when rbac.allowlist is empty"
else
  assert_contains "$OUTPUT" 'allowlist' \
    "expected validation error to reference rbac.allowlist"
fi

pass "Empty RBAC allowlist validation"

run_test "wildcard apiGroups is rejected"

if OUTPUT=$(render \
  --set-json 'rbac.allowlist=[{"apiGroups":["*"],"resources":["configmaps"]}]' 2>&1); then
  fail "expected helm template to fail when apiGroups contains wildcard"
else
  assert_contains "$OUTPUT" 'apiGroups' \
    "expected validation error to reference apiGroups"
fi

pass "Wildcard apiGroups validation"

run_test "wildcard resources is rejected"

if OUTPUT=$(render \
  --set-json 'rbac.allowlist=[{"apiGroups":[""],"resources":["*"]}]' 2>&1); then
  fail "expected helm template to fail when resources contains wildcard"
else
  assert_contains "$OUTPUT" 'resources' \
    "expected validation error to reference resources"
fi

pass "Wildcard resources validation"

run_test "empty apiGroups array is rejected"

if OUTPUT=$(render \
  --set-json 'rbac.allowlist=[{"apiGroups":[],"resources":["configmaps"]}]' 2>&1); then
  fail "expected helm template to fail when apiGroups is an empty array"
else
  assert_contains "$OUTPUT" 'apiGroups' \
    "expected validation error to reference apiGroups"
fi

pass "Empty apiGroups array validation"

run_test "empty resources array is rejected"

if OUTPUT=$(render \
  --set-json 'rbac.allowlist=[{"apiGroups":[""],"resources":[]}]' 2>&1); then
  fail "expected helm template to fail when resources is an empty array"
else
  assert_contains "$OUTPUT" 'resources' \
    "expected validation error to reference resources"
fi

pass "Empty resources array validation"

run_test "devModeWildcard with non-empty allowlist is rejected"

if OUTPUT=$(render \
  --set rbac.devModeWildcard=true \
  --set-json 'rbac.allowlist=[{"apiGroups":[""],"resources":["configmaps"]}]' 2>&1); then
  fail "expected helm template to fail when both devModeWildcard and allowlist are set"
else
  assert_contains "$OUTPUT" 'mutually exclusive' \
    "expected validation error to mention mutual exclusivity"
fi

pass "devModeWildcard + allowlist mutual exclusivity validation"

# ─── Summary ──────────────────────────────────────────────────────────

echo ""
if [ "$FAILED" -gt 0 ]; then
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "FAILED: $FAILED test(s) failed, $PASSED passed"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "All Helm chart tests passed! ($PASSED tests)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
