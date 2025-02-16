package stream

type Consumer interface {
	Consume(v any)
}

type ArrayConsumer[T any] struct {
	list []T
}

func NewArrayConsumer[T any]() *ArrayConsumer[T] {
	consumer := ArrayConsumer[T]{}
	return &consumer
}

func (c *ArrayConsumer[T]) Consume(v any) {
	c.list = append(c.list, v.(T))
}

func (c *ArrayConsumer[T]) Collect() []T {
	return c.list
}

type ChannelConsumer[T any] struct {
	ch chan<- T
}

func NewChannelConsumer[T any](ch chan<- T) *ChannelConsumer[T] {
	consumer := ChannelConsumer[T]{
		ch: ch,
	}
	return &consumer
}

func (c *ChannelConsumer[T]) Consume(v any) {
	c.ch <- v.(T)
}
