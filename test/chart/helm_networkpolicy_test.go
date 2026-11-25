package chart_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type networkPolicyDoc struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Ingress []struct {
			From []struct {
				NamespaceSelector *struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"namespaceSelector"`
				PodSelector *struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"podSelector"`
			} `yaml:"from"`
			Ports []struct {
				Port int `yaml:"port"`
			} `yaml:"ports"`
		} `yaml:"ingress"`
	} `yaml:"spec"`
}

func TestHelmNetworkPolicy_AdmitsTheLLMConsumerOnTheAPIPortOnly(t *testing.T) {
	out, err := helmRender(t, "gw")
	require.NoError(t, err, out)

	var found int
	for _, doc := range decodeAll[networkPolicyDoc](t, out) {
		if doc.Kind != "NetworkPolicy" {
			continue
		}
		for _, rule := range doc.Spec.Ingress {
			for _, from := range rule.From {
				if from.NamespaceSelector == nil || from.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "llm" {
					continue
				}
				found++
				require.NotNil(t, from.PodSelector, "the rule names the consumer's pods, not the whole namespace")
				assert.Equal(t, "llm-retrieval-generation", from.PodSelector.MatchLabels["app.kubernetes.io/name"])
				require.Len(t, rule.Ports, 1)
				assert.Equal(t, 8080, rule.Ports[0].Port, "the API port, not the metrics port")
			}
		}
	}
	assert.Equal(t, 1, found, "one ingress rule admits the LLM consumer")
}
