package mr

import (
	"sync"
	"time"

	"log"
)

const (
	TTL            = 10 * time.Millisecond
	BatchSize      = 128
	BufferInitSize = 128
	TokenSize      = 32
	QueueSize      = 1024
	PushTimeout    = time.Microsecond * 500
)

type Item = TaskID

// Inspired by Jaeger
type BatchBuffer struct {
	buffer  []Item
	queue   chan Item
	close   chan *sync.WaitGroup
	tokens  chan struct{}
	workers *sync.WaitGroup
	popper  func([]Item) error
	hook    func([]Item)
}

func New(popper func([]Item) error, hook func([]Item)) *BatchBuffer {
	return &BatchBuffer{
		buffer:  make([]Item, 0, BufferInitSize),
		queue:   make(chan Item, QueueSize),
		close:   make(chan *sync.WaitGroup),
		tokens:  make(chan struct{}, TokenSize),
		workers: &sync.WaitGroup{},
		popper:  popper,
		hook:    hook,
	}
}

func (b *BatchBuffer) pop(items []Item) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("PANIC in goroutine: %w", err)
		}
		<-b.tokens
		b.workers.Done()
	}()

	if err := b.popper(items); err != nil {
		log.Println("failed to pop items: %w", err)
	}
}

func (b *BatchBuffer) flush() {
	if len(b.buffer) == 0 {
		return
	}

	b.workers.Add(1)
	b.tokens <- struct{}{}

	go b.pop(b.buffer)

	b.buffer = make([]Item, 0, BufferInitSize)
}

func (b *BatchBuffer) Process() {
	ticker := time.NewTicker(TTL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()

		case item := <-b.queue:
			b.buffer = append(b.buffer, item)
			if b.hook != nil {
				b.hook(b.buffer)
			}

			if len(b.buffer) >= BatchSize {
				b.flush()
			}

		case wg := <-b.close:
			ticker.Stop()
			b.flush()
			wg.Done()
		}
	}
}

func (b *BatchBuffer) Push(item Item) {
	t := time.NewTimer(PushTimeout)
	defer t.Stop()

	select {
	case b.queue <- item:

	case <-t.C:
		log.Println("queue full")
		// TODO: retry
	}
}

func (b *BatchBuffer) Close() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	b.close <- wg
	wg.Wait()

	b.workers.Wait()
}
