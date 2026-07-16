package subscriber

import (
	"go.opentelemetry.io/otel/metric"
)

// meterName is the import path of this package. A Value's Name is appended to this to namespace its metrics.
const meterName = "github.com/gostdlib/base/values/generics/broadcast/subscriber"

// metrics are the OTEL instruments recorded by a Value that has a Name. The broadcast.Value behind each
// topic is left unnamed, as naming it would create a meter per topic pattern, so what a caller wants to
// know about the fan out is recorded here instead.
type metrics struct {
	meter metric.Meter
	// Sends is the number of values sent.
	Sends metric.Int64Counter
	// Matches is the number of (value, subscribed pattern) pairs a Send() fanned out to. A single Send()
	// records one of these for every pattern that matched its topic.
	Matches metric.Int64Counter
	// Unmatched is the number of values sent to a topic that no pattern matched, which is what a topic
	// that nobody subscribed to looks like.
	Unmatched metric.Int64Counter
	// Topics is the number of patterns currently subscribed to. Patterns are deduplicated, so two
	// subscribers to the same pattern are one topic.
	Topics metric.Int64UpDownCounter
	// Subscribers is the number of subscriptions currently held. A subscription is given back when its
	// Context is canceled, so this only falls when a subscriber cancels.
	Subscribers metric.Int64UpDownCounter
}

func newMetrics(m metric.Meter) *metrics {
	mets := &metrics{meter: m}

	var err error
	mets.Sends, err = m.Int64Counter("sends", metric.WithDescription("The number of values sent."))
	if err != nil {
		panic(err)
	}
	mets.Matches, err = m.Int64Counter("matches", metric.WithDescription("The number of subscribed patterns values were fanned out to."))
	if err != nil {
		panic(err)
	}
	mets.Unmatched, err = m.Int64Counter("unmatched", metric.WithDescription("The number of values sent to a topic no pattern matched."))
	if err != nil {
		panic(err)
	}
	mets.Topics, err = m.Int64UpDownCounter("topics", metric.WithDescription("The number of patterns currently subscribed to."))
	if err != nil {
		panic(err)
	}
	mets.Subscribers, err = m.Int64UpDownCounter("subscribers", metric.WithDescription("The number of subscriptions currently held."))
	if err != nil {
		panic(err)
	}

	return mets
}
