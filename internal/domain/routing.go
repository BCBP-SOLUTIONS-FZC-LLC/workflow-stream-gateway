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
	Source string                `yaml:"source"`
	Events map[string]EventRoute `yaml:"events"`
}

type EventRoute struct {
	Recipients []string `yaml:"recipients"`
	Department string   `yaml:"department,omitempty"`
}

type Route struct {
	Service    string
	EventType  string
	Recipients []string
	Department string
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
		for _, eventType := range sortedKeys(svc.Events) {
			ev := svc.Events[eventType]
			if err := checkEvent(name, eventType, ev); err != nil {
				return nil, err
			}
			t.routes[routeKey{svc.Source, eventType}] = Route{
				Service:    name,
				EventType:  eventType,
				Recipients: append([]string(nil), ev.Recipients...),
				Department: ev.Department,
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
	if len(svc.Events) == 0 {
		return fmt.Errorf("service %q: at least one event is required", name)
	}
	if _, empty := svc.Events[""]; empty {
		return fmt.Errorf("service %q: an event type is empty", name)
	}
	return nil
}

func checkEvent(service, eventType string, ev EventRoute) error {
	if len(ev.Recipients) == 0 {
		return fmt.Errorf("service %q event %q: at least one recipient field is required", service, eventType)
	}
	seen := map[string]bool{}
	for _, field := range ev.Recipients {
		if field == "" {
			return fmt.Errorf("service %q event %q: a recipient field is empty", service, eventType)
		}
		if seen[field] {
			return fmt.Errorf("service %q event %q: recipient field %q is listed twice", service, eventType, field)
		}
		seen[field] = true
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
