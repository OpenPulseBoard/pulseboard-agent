{{/* Common name + labels helpers */}}
{{- define "pulseagent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pulseagent.fullname" -}}
{{- printf "%s" (include "pulseagent.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pulseagent.labels" -}}
app.kubernetes.io/name: {{ include "pulseagent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "pulseagent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pulseagent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Name of the secret holding connection credentials */}}
{{- define "pulseagent.secretName" -}}
{{- if .Values.pulseboard.existingSecret -}}
{{- .Values.pulseboard.existingSecret -}}
{{- else -}}
{{- printf "%s-enroll" (include "pulseagent.fullname" .) -}}
{{- end -}}
{{- end -}}
