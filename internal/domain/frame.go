package domain

type Frame struct {
	ID        string
	EventType string
	Data      []byte
	SourceKey string
}
