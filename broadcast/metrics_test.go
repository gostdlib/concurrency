package broadcast

import (
	"iter"
	"testing"

	"github.com/gostdlib/base/context"
	baseMetrics "github.com/gostdlib/base/telemetry/otel/metrics"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricReader wires a Context to a fresh ManualReader-backed MeterProvider so a test can read the metrics a
// named Value emits. It returns the Context to pass to the Value and the reader to collect from.
func metricReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	baseMetrics.Set(mp)
	return reader
}

// instruments are every metric this package records. A counter that has never been added to is not exported
// at all, so a metric this test cannot find reads as 0, which is right for a counter that was never touched
// but would also quietly pass a want of 0 against a name that does not exist. Checking the name against this
// set is what keeps a renamed or misspelled metric from turning every such want into a test that proves nothing.
var instruments = map[string]bool{
	"sends":       true,
	"delivered":   true,
	"subscribers": true,
	"pending":     true,
}

// sumValue collects the current value of the named int64 sum metric (counter or up/down counter) from reader.
func sumValue(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, testFunc, name, metricName string) int64 {
	t.Helper()

	if !instruments[metricName] {
		t.Fatalf("%s(%s): metric(%s) is not one this package records", testFunc, name, metricName)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("%s(%s): could not collect metrics: %s", testFunc, name, err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, item := range sm.Metrics {
			if item.Name != metricName {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			total := int64(0)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func TestMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// consume is how many values the subscriber takes before it breaks out. A negative value takes
		// everything and a 0 means no one subscribes at all.
		consume       int
		sends         int
		wantSends     int64
		wantDelivered int64
	}{
		{
			name:          "Success: a subscriber that drains everything leaves nothing pending",
			sends:         3,
			consume:       -1,
			wantSends:     3,
			wantDelivered: 3,
		},
		{
			name:          "Success: a subscriber that breaks out early leaves nothing pending",
			sends:         5,
			consume:       1,
			wantSends:     5,
			wantDelivered: 1,
		},
		{
			name:          "Success: sends with no subscriber are counted but never pending",
			sends:         3,
			consume:       0,
			wantSends:     3,
			wantDelivered: 0,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		reader := metricReader(t)

		v := &Value[int]{Name: "test"}

		var seq iter.Seq[int]
		if test.consume != 0 {
			seq = v.Subscribe(ctx)
		}

		for i := 0; i < test.sends; i++ {
			v.Send(ctx, i)
		}
		v.Close(ctx)

		if seq != nil {
			taken := 0
			for range seq {
				taken++
				if test.consume > 0 && taken == test.consume {
					break
				}
			}
		}

		// Once every subscriber has stopped, nothing can still be pending on one and no one is subscribed.
		wants := map[string]int64{"sends": test.wantSends, "delivered": test.wantDelivered, "pending": 0, "subscribers": 0}
		for metricName, want := range wants {
			if got := sumValue(t, ctx, reader, "TestMetrics", test.name, metricName); got != want {
				t.Errorf("TestMetrics(%s): %s: got %d, want %d", test.name, metricName, got, want)
			}
		}
	}
}

// TestMetricsPending checks that pending rises while a subscriber is not keeping up with Send() and falls back
// to zero once that subscriber stops, which is what makes it a usable signal for a slow subscriber.
func TestMetricsPending(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	reader := metricReader(t)

	v := &Value[int]{Name: "test"}
	next, stop := iter.Pull(v.Subscribe(ctx))

	v.Send(ctx, 1)
	if _, ok := next(); !ok {
		t.Fatalf("TestMetricsPending: got no value from the subscription, want 1")
	}

	// The subscriber is now parked in its iterator and is not taking anything else.
	for i := 0; i < 3; i++ {
		v.Send(ctx, i)
	}

	if got := sumValue(t, ctx, reader, "TestMetricsPending", "stalled", "pending"); got != 3 {
		t.Errorf("TestMetricsPending: pending while stalled: got %d, want 3", got)
	}

	stop()

	if got := sumValue(t, ctx, reader, "TestMetricsPending", "stopped", "pending"); got != 0 {
		t.Errorf("TestMetricsPending: pending after the subscriber stopped: got %d, want 0", got)
	}
	if got := sumValue(t, ctx, reader, "TestMetricsPending", "stopped", "subscribers"); got != 0 {
		t.Errorf("TestMetricsPending: subscribers after the subscriber stopped: got %d, want 0", got)
	}
}
