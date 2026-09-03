package events

import (
	"testing"
	"time"
)

func TestBrokerBoundsSlowProjectionSubscriber(t *testing.T) {
	broker := NewBroker()
	stream, unsubscribe := broker.Subscribe()
	defer unsubscribe()

	for index := 0; index < cap(stream); index++ {
		broker.Publish(AssistantDelta, map[string]any{"delta": "x"})
	}
	if got := len(stream); got != cap(stream) {
		t.Fatalf("subscriber queue length = %d, want %d", got, cap(stream))
	}

	// A terminal event must not be silently dropped, but it may wait for a
	// slow subscriber. Reading one queued projection event releases capacity.
	done := make(chan struct{})
	go func() {
		broker.Publish(TurnCompleted, map[string]any{"status": "completed"})
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("reliable publish completed without subscriber backpressure")
	case <-time.After(25 * time.Millisecond):
	}
	<-stream
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reliable publish did not resume after subscriber consumed an event")
	}
}

func TestBrokerUnsubscribeReleasesReliablePublisher(t *testing.T) {
	broker := NewBroker()
	stream, unsubscribe := broker.Subscribe()
	for index := 0; index < cap(stream); index++ {
		broker.Publish(AssistantDelta, map[string]any{"delta": "x"})
	}

	done := make(chan struct{})
	go func() {
		broker.Publish(InteractionResolved, map[string]any{"id": "interaction-1"})
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("reliable publish should wait while the subscriber queue is full")
	case <-time.After(25 * time.Millisecond):
	}

	unsubscribe()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe did not release the blocked reliable publisher")
	}
}
