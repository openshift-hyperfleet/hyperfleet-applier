{{/*
Expand the name of the chart.
*/}}
{{- define "hyperfleet-applier.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hyperfleet-applier.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hyperfleet-applier.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hyperfleet-applier.labels" -}}
helm.sh/chart: {{ include "hyperfleet-applier.chart" . }}
{{ include "hyperfleet-applier.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hyperfleet-applier.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperfleet-applier.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hyperfleet-applier.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hyperfleet-applier.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}



{{/*
Create full container image name
*/}}
{{- define "hyperfleet-applier.fullImageName" -}}
{{- if or (not .Values.image.registry) (not .Values.image.repository) (not
.Values.image.tag) -}}
{{- fail "Registry, repository and tag are required" }}
{{- else -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}
{{- end }}


{{/*
Validate required values
*/}}
{{- define "hyperfleet-applier.validateValues" -}}
{{- if not .Values.applier.managementCluster }}
{{- fail "applier.managementCluster is required" }}
{{- end }}
{{- if not .Values.applier.pollInterval }}
{{- fail "applier.pollInterval is required" }}
{{- end }}
{{- if not .Values.redis.address }}
{{- fail "redis.address is required" }}
{{- end }}
{{- if and .Values.rbac.create (not .Values.rbac.devModeWildcard) (not .Values.rbac.allowlist) }}
{{- fail "rbac.allowlist must not be empty when rbac.create=true and rbac.devModeWildcard=false. Populate an explicit GVR allowlist, or set rbac.devModeWildcard=true for local/dev only (see chart README warning)." }}
{{- end }}
{{- if and .Values.rbac.create .Values.rbac.devModeWildcard .Values.rbac.allowlist }}
{{- fail "rbac.devModeWildcard=true and rbac.allowlist are mutually exclusive. The wildcard silently overrides the allowlist — remove the allowlist entries or set rbac.devModeWildcard=false." }}
{{- end }}
{{- range $i, $entry := .Values.rbac.allowlist }}
{{- if not $entry.apiGroups }}
{{- fail (printf "rbac.allowlist[%d].apiGroups must not be empty — specify at least one API group (use \"\" for the core group)." $i) }}
{{- end }}
{{- if not $entry.resources }}
{{- fail (printf "rbac.allowlist[%d].resources must not be empty — specify at least one resource type." $i) }}
{{- end }}
{{- range $entry.apiGroups }}
{{- if eq . "*" }}
{{- fail (printf "rbac.allowlist[%d].apiGroups contains \"*\". Wildcards are not allowed in the explicit allowlist — use rbac.devModeWildcard=true for dev/local only." $i) }}
{{- end }}
{{- end }}
{{- range $entry.resources }}
{{- if eq . "*" }}
{{- fail (printf "rbac.allowlist[%d].resources contains \"*\". Wildcards are not allowed in the explicit allowlist — use rbac.devModeWildcard=true for dev/local only." $i) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

