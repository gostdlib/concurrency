package subscriber

import (
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

// instruments are every metric this package records. A counter that has never been added to is not
// exported at all, so a metric this test cannot find reads as 0, which is right for a counter that was
// never touched but would also quietly pass a want of 0 against a name that does not exist. Checking the
// name against this set is what keeps a renamed or misspelled metric from turning every such want into a
// test that proves nothing.
var instruments = map[string]bool{
	"sends":       true,
	"matches":     true,
	"unmatched":   true,
	"topics":      true,
	"subscribers": true,
}

// sumValue collects the current value of the named int64 sum metric (counter or up/down counter) from
// reader. A metric that is not exported yet is 0, as an instrument only materializes on its first record.
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
		name          string
		patterns      []string
		sends         []send
		wantSends     int64
		wantMatches   int64
		wantUnmatched int64
		wantTopics    int64
	}{
		{
			name:          "Success: a send that one pattern matches",
			patterns:      []string{"prices/**"},
			sends:         []send{{"prices/us", 1}},
			wantSends:     1,
			wantMatches:   1,
			wantUnmatched: 0,
			wantTopics:    1,
		},
		{
			name:          "Success: a send that three patterns match is three matches",
			patterns:      []string{"prices/us/nyse", "prices/us/*", "prices/**"},
			sends:         []send{{"prices/us/nyse", 1}},
			wantSends:     1,
			wantMatches:   3,
			wantUnmatched: 0,
			wantTopics:    3,
		},
		{
			name:          "Success: a send no pattern matches is unmatched",
			patterns:      []string{"prices/**"},
			sends:         []send{{"trades/us", 1}},
			wantSends:     1,
			wantMatches:   0,
			wantUnmatched: 1,
			wantTopics:    1,
		},
		{
			name:          "Success: two subscriptions to one pattern are one topic",
			patterns:      []string{"prices/**", "prices/**"},
			sends:         []send{{"prices/us", 1}, {"prices/us", 2}},
			wantSends:     2,
			wantMatches:   2,
			wantUnmatched: 0,
			wantTopics:    1,
		},
	}

	for _, test := range tests {
		ctx := t.Context()
		reader := metricReader(t)

		v := &Value[int]{Name: "test"}
		for _, p := range test.patterns {
			if _, err := v.Subscribe(ctx, p); err != nil {
				t.Fatalf("TestMetrics(%s): Subscribe(%s): got err == %s, want err == nil", test.name, p, err)
			}
		}

		for _, s := range test.sends {
			if err := v.Send(ctx, s.topic, s.value); err != nil {
				t.Fatalf("TestMetrics(%s): Send(%s): got err == %s, want err == nil", test.name, s.topic, err)
			}
		}

		wants := map[string]int64{
			"sends":       test.wantSends,
			"matches":     test.wantMatches,
			"unmatched":   test.wantUnmatched,
			"topics":      test.wantTopics,
			"subscribers": int64(len(test.patterns)),
		}
		for metricName, want := range wants {
			if got := sumValue(t, ctx, reader, "TestMetrics", test.name, metricName); got != want {
				t.Errorf("TestMetrics(%s): %s: got %d, want %d", test.name, metricName, got, want)
			}
		}

		// Close() ends every subscription, whether or not its Context is canceled, and no Context is canceled
		// in this test. So both gauges have to come back to 0 on Close() alone, which is what keeps a Value
		// that is closed while it still has subscribers from leaving the count high forever.
		v.Close(ctx)
		for _, metricName := range []string{"topics", "subscribers"} {
			if got := sumValue(t, ctx, reader, "TestMetrics", test.name, metricName); got != 0 {
				t.Errorf("TestMetrics(%s): %s after Close(): got %d, want 0", test.name, metricName, got)
			}
		}
	}
}

// TestMetricsRelease checks that the topics and subscribers gauges fall back as subscribers cancel, so
// that a Value which everyone has walked away from reports as empty rather than drifting upward.
func TestMetricsRelease(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	reader := metricReader(t)

	v := &Value[int]{Name: "test"}
	defer v.Close(ctx)

	first, cancelFirst := context.WithCancel(ctx)
	second, cancelSecond := context.WithCancel(ctx)

	for _, sub := range []struct {
		ctx     context.Context
		pattern string
	}{
		{first, "prices/**"},
		{second, "prices/**"},
	} {
		if _, err := v.Subscribe(sub.ctx, sub.pattern); err != nil {
			t.Fatalf("TestMetricsRelease: Subscribe(%s): got err == %s, want err == nil", sub.pattern, err)
		}
	}

	if got := sumValue(t, ctx, reader, "TestMetricsRelease", "subscribed", "subscribers"); got != 2 {
		t.Errorf("TestMetricsRelease: subscribers: got %d, want 2", got)
	}
	if got := sumValue(t, ctx, reader, "TestMetricsRelease", "subscribed", "topics"); got != 1 {
		t.Errorf("TestMetricsRelease: topics: got %d, want 1", got)
	}

	// One subscriber leaves. The pattern still has one, so it stays.
	cancelFirst()
	waitTopics(t, v, 1)
	if got := sumValue(t, ctx, reader, "TestMetricsRelease", "one left", "topics"); got != 1 {
		t.Errorf("TestMetricsRelease: topics after one of two subscribers left: got %d, want 1", got)
	}

	// The last subscriber leaves, so both gauges fall back to zero.
	cancelSecond()
	waitTopics(t, v, 0)
	if got := sumValue(t, ctx, reader, "TestMetricsRelease", "all left", "topics"); got != 0 {
		t.Errorf("TestMetricsRelease: topics after every subscriber left: got %d, want 0", got)
	}
	if got := sumValue(t, ctx, reader, "TestMetricsRelease", "all left", "subscribers"); got != 0 {
		t.Errorf("TestMetricsRelease: subscribers after every subscriber left: got %d, want 0", got)
	}
}
