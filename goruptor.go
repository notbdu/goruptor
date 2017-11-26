package goruptor

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const MaxSequence = (1 << 63) - 1

type Consumer interface {
	Consume(value interface{})
}

type wrappedConsumer struct {
	consumer Consumer
	sequence int64
}

type Disruptor interface {
	// Start consuming output
	Start()
	Publish(value interface{})
	AddConsumers(...Consumer) error
}

func New(rbsize int64) (Disruptor, error) {
	if rbsize > 0 && (rbsize&(rbsize-1)) != 0 {
		return nil, fmt.Errorf("%d is not a power of 2", rbsize)
	}

	return &disruptor{
		ringBuffer: make([]interface{}, rbsize),
		rbsize:     rbsize,
		sequence:   0,
		isStopped:  true,
	}, nil
}

type disruptor struct {
	sync.Mutex
	ringBuffer []interface{}
	rbsize     int64
	consumers  []*wrappedConsumer
	sequence   int64
	isStopped  bool
}

func (d *disruptor) Start() {
	d.isStopped = false
	for _, wc := range d.consumers {
		go func(wc *wrappedConsumer) {
			for {
				// Loop if we've caught up
				sequence := atomic.LoadInt64(&wc.sequence)
				if sequence+1 == atomic.LoadInt64(&d.sequence) {
					continue
				}
				atomic.StoreInt64(&wc.sequence, sequence+1)
				wc.consumer.Consume(d.ringBuffer[wc.sequence&(d.rbsize-1)])
				if d.isStopped {
					break
				}
			}
		}(wc)
	}
}

func (d *disruptor) Stop() {
	d.isStopped = true
}

func (d *disruptor) getSlowestConsumer() int64 {
	slowestConsumer := int64(MaxSequence)
	for _, wc := range d.consumers {
		sequence := atomic.LoadInt64(&wc.sequence)
		if sequence < slowestConsumer {
			slowestConsumer = sequence
		}
	}
	return slowestConsumer
}

func (d *disruptor) Publish(value interface{}) {
	for {
		sequence := atomic.LoadInt64(&d.sequence)
		// Make sure we don't wrap
		if (sequence+1)-d.getSlowestConsumer() > d.rbsize {
			continue
		}
		d.ringBuffer[sequence&(d.rbsize-1)] = value
		atomic.StoreInt64(&d.sequence, sequence+1)
		break
	}
}

func (d *disruptor) AddConsumers(consumers ...Consumer) error {
	d.Lock()
	defer d.Unlock()
	for _, c := range consumers {
		d.consumers = append(d.consumers, &wrappedConsumer{
			consumer: c,
			sequence: 0,
		})
	}
	return nil
}
