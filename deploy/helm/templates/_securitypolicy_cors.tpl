{{- define "workflow-stream-gateway.cors" -}}
cors:
  allowOrigins: {{ if .Values.ingress.securityPolicy.cors.allowOrigins }}
    {{- range .Values.ingress.securityPolicy.cors.allowOrigins }}
    - type: Exact
      value: {{ . | quote }}
    {{- end }}
  {{- else }} []
  {{- end }}
  allowMethods:
    {{- toYaml .Values.ingress.securityPolicy.cors.allowMethods | nindent 4 }}
  allowHeaders:
    {{- toYaml .Values.ingress.securityPolicy.cors.allowHeaders | nindent 4 }}
  maxAge: {{ .Values.ingress.securityPolicy.cors.maxAge | quote }}
{{- end }}
