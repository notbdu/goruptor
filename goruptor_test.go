package goruptor

import (
	"log"
	"testing"
)

type TestConsumer struct{}

func (c *TestConsumer) Consume(value interface{}) {
	val := value.(string)
	log.Println("consumed:", val)
}

func TestDisruptor(t *testing.T) {
	disruptor, err := New(1 << 2)
	if err != nil {
		t.Error(err)
	}
	disruptor.Publish("hello")
	disruptor.Publish("hello1")
	disruptor.Publish("hello2")
	disruptor.Publish("hello3")
	disruptor.AddConsumers(&TestConsumer{}, &TestConsumer{})
	disruptor.Start()
	log.Println("after start")
	disruptor.Publish("hello4")
	disruptor.Publish("hello5")
	disruptor.Publish("hello6")
	disruptor.Publish("hello7")
}
