package domain

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
)

const RoutingFormatVersion = 1

type RoutingFile struct {
	Version  int                      `yaml:"version"`
	Services map[string]ServiceRoutes `yaml:"services"`
}

type ServiceRoutes struct {
	Source       string   `yaml:"source"`
	TenantAdmins *bool    `yaml:"tenant_admins"`
	Events       []string `yaml:"events"`
}

type Route struct {
	Service      string
	EventType    string
	TenantAdmins bool
}

type RecipientKind string

const (
	RecipientUser       RecipientKind = "user"
	RecipientDepartment RecipientKind = "department"
)

const MaxRecipientIDLength = 128

type Recipient struct {
	Kind RecipientKind `json:"kind"`
	ID   string        `json:"id"`
}

func (r Recipient) Validate() error {
	if r.Kind != RecipientUser && r.Kind != RecipientDepartment {
		return fmt.Errorf("kind %q must be user or department", r.Kind)
	}
	if r.ID == "" || len(r.ID) > MaxRecipientIDLength {
		return fmt.Errorf("id must be 1 to %d characters", MaxRecipientIDLength)
	}
	return nil
}

func (r Route) StreamEventType() string {
	return r.Service + "." + r.EventType
}

type routeKey struct {
	source    string
	eventType string
}

type RoutingTable struct {
	routes   map[routeKey]Route
	services map[string][]string
}

var serviceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

var eventTypePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]*$`)

func NewRoutingTable(f RoutingFile) (*RoutingTable, error) {
	if f.Version != RoutingFormatVersion {
		return nil, fmt.Errorf("version must be %d, got %d", RoutingFormatVersion, f.Version)
	}
	if len(f.Services) == 0 {
		return nil, errors.New("at least one service is required")
	}
	t := &RoutingTable{routes: map[routeKey]Route{}, services: map[string][]string{}}
	sources := map[string]string{}
	for _, name := range sortedKeys(f.Services) {
		svc := f.Services[name]
		if err := checkService(name, svc, sources); err != nil {
			return nil, err
		}
		events := append([]string(nil), svc.Events...)
		sort.Strings(events)
		for i, eventType := range events {
			if err := checkEvent(name, eventType); err != nil {
				return nil, err
			}
			if i > 0 && events[i-1] == eventType {
				return nil, fmt.Errorf("service %q: event type %q is listed twice", name, eventType)
			}
			t.routes[routeKey{svc.Source, eventType}] = Route{
				Service:      name,
				EventType:    eventType,
				TenantAdmins: *svc.TenantAdmins,
			}
			t.services[name] = append(t.services[name], eventType)
		}
	}
	return t, nil
}

func checkService(name string, svc ServiceRoutes, sources map[string]string) error {
	if !serviceNamePattern.MatchString(name) {
		return fmt.Errorf("service %q: name must be lowercase letters, digits and dashes", name)
	}
	if svc.Source == "" {
		return fmt.Errorf("service %q: source is required", name)
	}
	if other, taken := sources[svc.Source]; taken {
		return fmt.Errorf("source %q is used by two services (%s, %s)", svc.Source, other, name)
	}
	sources[svc.Source] = name
	if svc.TenantAdmins == nil {
		return fmt.Errorf("service %q: tenant_admins is required", name)
	}
	if len(svc.Events) == 0 {
		return fmt.Errorf("service %q: at least one event is required", name)
	}
	return nil
}

func checkEvent(service, eventType string) error {
	if eventType == "" {
		return fmt.Errorf("service %q: an event type is empty", service)
	}
	if !eventTypePattern.MatchString(eventType) {
		return fmt.Errorf("service %q: event type %q must be letters, digits, dots, dashes and underscores", service, eventType)
	}
	return nil
}

func (t *RoutingTable) Lookup(source, eventType string) (Route, bool) {
	r, ok := t.routes[routeKey{source, eventType}]
	return r, ok
}

func (t *RoutingTable) EventTypes(service string) []string {
	return append([]string(nil), t.services[service]...)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
