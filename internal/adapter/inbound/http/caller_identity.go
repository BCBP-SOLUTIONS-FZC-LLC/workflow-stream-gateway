package http

import (
	"regexp"
	"strings"
)

const clientCertHeader = "X-Forwarded-Client-Cert"

var spiffeServiceAccount = regexp.MustCompile(`^spiffe://[^/]+/ns/([^/]+)/sa/([^/]+)$`)

func callerFromClientCert(header string) string {
	elements := splitOutsideQuotes(header, ',')
	if len(elements) == 0 {
		return ""
	}
	for _, field := range splitOutsideQuotes(elements[len(elements)-1], ';') {
		key, value, ok := strings.Cut(field, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "URI") {
			continue
		}
		m := spiffeServiceAccount.FindStringSubmatch(strings.Trim(strings.TrimSpace(value), `"`))
		if m == nil {
			return ""
		}
		return m[1] + "/" + m[2]
	}
	return ""
}

func splitOutsideQuotes(s string, sep rune) []string {
	if s == "" {
		return nil
	}
	var parts []string
	var b strings.Builder
	quoted := false
	for _, r := range s {
		switch {
		case r == '"':
			quoted = !quoted
			b.WriteRune(r)
		case r == sep && !quoted:
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	return append(parts, b.String())
}
