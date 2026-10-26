{{- define "arneb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "arneb.fullname" -}}
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

{{- define "arneb.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "arneb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Selector labels; pass (dict "ctx" $ "component" "coordinator"). */}}
{{- define "arneb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arneb.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/* Labels plus the component; pass (dict "ctx" $ "component" "worker"). */}}
{{- define "arneb.componentLabels" -}}
{{ include "arneb.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "arneb.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "arneb.clusterSecretName" -}}
{{- .Values.clusterSecret.existingSecret | default (printf "%s-cluster" (include "arneb.fullname" .)) }}
{{- end }}

{{- define "arneb.clusterSecretKey" -}}
{{- if .Values.clusterSecret.existingSecret }}{{ .Values.clusterSecret.key }}{{ else }}secret{{ end }}
{{- end }}

{{/* Env, volumes and mounts every pod shares. */}}
{{- define "arneb.commonEnv" -}}
- name: RUST_LOG
  value: {{ .Values.logLevel | quote }}
- name: ARNEB_CLUSTER_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "arneb.clusterSecretName" . }}
      key: {{ include "arneb.clusterSecretKey" . }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "arneb.volumes" -}}
- name: config
{{- if .Values.existingConfigSecret }}
  secret:
    secretName: {{ .Values.existingConfigSecret }}
{{- else }}
  configMap:
    name: {{ include "arneb.fullname" . }}-config
{{- end }}
{{- with .Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "arneb.volumeMounts" -}}
- name: config
  mountPath: /etc/arneb
  readOnly: true
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "arneb.configChecksum" -}}
{{- if .Values.existingConfigSecret }}{{ .Values.existingConfigSecret | sha256sum }}{{ else }}{{ .Values.config | sha256sum }}{{ end }}
{{- end }}

{{/* Pod-level fields every pod shares. */}}
{{- define "arneb.podCommon" -}}
automountServiceAccountToken: false
# Service links would inject ARNEB_PORT=tcp://... for a Service named
# "arneb", which arneb reads as its pgwire port.
enableServiceLinks: false
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
securityContext:
  {{- toYaml .Values.podSecurityContext | nindent 2 }}
{{- end }}

{{/* nodeSelector, tolerations and affinity; pass .Values.coordinator or .Values.worker. */}}
{{- define "arneb.scheduling" -}}
{{- $v := . }}
{{- range $k := list "nodeSelector" "tolerations" "affinity" }}
{{- with index $v $k }}
{{ $k }}:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}
