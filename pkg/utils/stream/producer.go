package stream

type Producer interface {
	Produce() (any, bool)
}

type ArrayProducer[T any] struct {
	list []T
}

func NewArrayProducer[T any](list []T) *ArrayProducer[T] {
	producer := ArrayProducer[T]{
		list: list,
	}
	return &producer
}

func (p *ArrayProducer[T]) Produce() (any, bool) {
	if len(p.list) == 0 {
		return nil, false
	}
	v := p.list[0]
	p.list = p.list[1:]
	return v, true
}

type ChannelProducer[T any] struct {
	ch <-chan T
}

func NewChannelProducer[T any](ch <-chan T) *ChannelProducer[T] {
	producer := ChannelProducer[T]{
		ch: ch,
	}
	return &producer
}

func (p *ChannelProducer[T]) Produce() (any, bool) {
	if p.ch == nil {
		return nil, false
	}

	v, ok := <-p.ch
	return v, ok
}
