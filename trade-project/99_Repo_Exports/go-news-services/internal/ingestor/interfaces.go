package ingestor

import "context"

type RSSSource interface {
	Fetch(ctx context.Context) ([]NewsRawItem, error)
}

type CalendarSource interface {
	Fetch(ctx context.Context) ([]CalendarEvent, error)
}

// Заглушка календаря (чтобы пайплайн компилился и работал)
type noopCalendarSource struct{ name string }

func NewNoopCalendarSource(name string) CalendarSource { return &noopCalendarSource{name: name} }
func (n *noopCalendarSource) Fetch(ctx context.Context) ([]CalendarEvent, error) {
	return nil, nil
}
