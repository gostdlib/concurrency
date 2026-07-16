package broadcast

import (
	"go.opentelemetry.io/otel/metric"
)

// meterName is the import path of this package. A Value's Name is appended to this to namespace its metrics.
const meterName = "github.com/gostdlib/base/values/generics/broadcast"

// metrics are the OTEL instruments recorded by a Value that has a Name.
type metrics struct {
	meter metric.Meter
	// Sends is the number of values sent.
	Sends metric.Int64Counter
	// Delivered is the number of values handed to a subscriber.
	Delivered metric.Int64Counter
	// Subscribers is the number of subscriptions currently being iterated.
	Subscribers metric.Int64UpDownCounter
	// Pending is the number of values that have been sent but not yet delivered, summed across every
	// subscriber. This is the memory that slow subscribers are holding onto: if it climbs and does not fall
	// back, a subscriber is not keeping up with Send(). Subscribing and unsubscribing race with Send(), so
	// this can be off by the number of values sent during those calls. It is a signal, not a ledger.
	Pending metric.Int64UpDownCounter
}

func newMetrics(m metric.Meter) *metrics {
	mets := &metrics{meter: m}

	var err error
	mets.Sends, err = m.Int64Counter("sends", metric.WithDescription("The number of values sent."))
	if err != nil {
		panic(err)
	}
	mets.Delivered, err = m.Int64Counter("delivered", metric.WithDescription("The number of values handed to a subscriber."))
	if err != nil {
		panic(err)
	}
	mets.Subscribers, err = m.Int64UpDownCounter("subscribers", metric.WithDescription("The number of subscriptions being iterated."))
	if err != nil {
		panic(err)
	}
	mets.Pending, err = m.Int64UpDownCounter("pending", metric.WithDescription("The number of values sent but not yet delivered."))
	if err != nil {
		panic(err)
	}

	return mets
}
