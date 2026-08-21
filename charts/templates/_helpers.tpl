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
{{- end }}

