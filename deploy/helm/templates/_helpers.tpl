{{/*
Expand the name of the chart.
*/}}
{{- define "workflow-stream-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "workflow-stream-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "workflow-stream-gateway.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "workflow-stream-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels — used by Deployment, Service, HPA, and PDB.
*/}}
{{- define "workflow-stream-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "workflow-stream-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image tag: prefer .Values.image.tag; fall back to chart appVersion.
*/}}
{{- define "workflow-stream-gateway.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
With the policy off, the NetworkPolicy alone fronts the internal route: refused in staging and prod unless accepted.
*/}}
{{- define "workflow-stream-gateway.validateAuthorizationPolicy" -}}
{{- if and (not .Values.authorizationPolicy.enabled) (has .Values.env.APP_ENV (list "prod" "staging")) (not .Values.authorizationPolicy.allowDisabled) -}}
{{- fail (printf "authorizationPolicy.enabled is false while env.APP_ENV is %s: only the NetworkPolicy would stand in front of the internal route. Enable the policy, or set authorizationPolicy.allowDisabled=true to accept that" .Values.env.APP_ENV) -}}
{{- end -}}
{{- end }}
