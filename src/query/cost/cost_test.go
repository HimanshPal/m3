// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//

package cost

import (
	"testing"

	"github.com/m3db/m3/src/x/cost"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestPerQueryEnforcerFactory_New(t *testing.T) {
	t.Run("creates independent local enforcers with shared global enforcer", func(t *testing.T) {
		globalEnforcer := newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true})
		localEnforcer := newTestEnforcer(cost.Limit{Threshold: 5.0, Enabled: true})

		pef := NewPerQueryEnforcerFactory(globalEnforcer, localEnforcer, nil)

		l1, l2 := pef.New(), pef.New()

		l1.Add(2)

		assertCurCost(t, 2.0, l1)
		assertCurCost(t, 0.0, l2)
		assertCurCost(t, 2.0, globalEnforcer)
	})
}

func TestPerQueryEnforcer_Report(t *testing.T) {
	testScope := tally.NewTestScope("", nil)

	pef := NewPerQueryEnforcerFactory(
		newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true}),
		newTestEnforcer(cost.Limit{Threshold: 5.0, Enabled: true}),
		NewPerQueryEnforcerOpts(instrument.NewOptions().SetMetricsScope(testScope)).SetDatapointsDistroBuckets(
			tally.ValueBuckets{5.0, 10.0}))

	pqe1, pqe2 := pef.New(), pef.New()
	pqe1.Add(cost.Cost(1.0))
	pqe2.Add(cost.Cost(6.0))

	pqe1.Report()
	pqe2.Report()

	snapshot := testScope.Snapshot()

	globalGauge := requireGauge(t, snapshot.Gauges(),
		tally.KeyForPrefixedStringMap("global.datapoints", map[string]string{}))

	assert.Equal(t, 7.0, globalGauge.Value())

	localHisto := requireHistogram(t, snapshot.Histograms(),
		tally.KeyForPrefixedStringMap("per-query.datapoints-distro", map[string]string{}))

	const delta = 0.00001
	assert.InDelta(t, 1, localHisto.Values()[5.0], delta)
	assert.InDelta(t, 1, localHisto.Values()[10.0], delta)
}

func TestPerQueryEnforcer_Release(t *testing.T) {
	t.Run("removes local total from global", func(t *testing.T) {
		pef := NewPerQueryEnforcerFactory(
			newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true}),
			newTestEnforcer(cost.Limit{Threshold: 5.0, Enabled: true}),
			nil)

		pqe1, pqe2 := pef.New(), pef.New()

		pqe1.Add(cost.Cost(5.0))
		pqe1.Add(cost.Cost(6.0))

		pqe2.Add(cost.Cost(7.0))

		pqe1.Release()

		assertCurCost(t, cost.Cost(7.0), pef.GlobalEnforcer())
		pqe2.Release()
		assertCurCost(t, cost.Cost(0.0), pef.GlobalEnforcer())
	})
}

func TestPerQueryEnforcer_Add(t *testing.T) {
	assertGlobalError := func(t *testing.T, err error) {
		if assert.Error(t, err) {
			assert.Regexp(t, "exceeded global limit", err.Error())
		}
	}

	assertLocalError := func(t *testing.T, err error) {
		if assert.Error(t, err) {
			assert.Regexp(t, "exceeded per query limit", err.Error())
		}
	}

	t.Run("errors on global error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(5.0, 100.0)
		r := pqe.Add(cost.Cost(6.0))
		assertGlobalError(t, r.Error)
	})

	t.Run("errors on local error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(100.0, 5.0)
		r := pqe.Add(cost.Cost(6.0))
		assertLocalError(t, r.Error)
	})

	t.Run("adds to local in case of global error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(5.0, 100.0)
		r := pqe.Add(cost.Cost(6.0))
		assertGlobalError(t, r.Error)

		r, _ = pqe.State()
		assert.Equal(t, cost.Report{
			Error: nil,
			Cost:  6.0},
			r)
	})

	t.Run("adds to global in case of local error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(100.0, 5.0)
		r := pqe.Add(cost.Cost(6.0))
		assertLocalError(t, r.Error)

		r, _ = pqe.global.State()
		assert.Equal(t, cost.Report{
			Error: nil,
			Cost:  6.0},
			r)
	})

	t.Run("release after local error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(10.0, 5.0)

		// exceeds local
		r := pqe.Add(6.0)
		assertLocalError(t, r.Error)

		pqe.Release()
		assertCurCost(t, 0.0, pqe.global)
	})

	t.Run("release after global error", func(t *testing.T) {
		pqe := newTestPerQueryEnforcer(5.0, 10.0)
		// exceeds global
		r := pqe.Add(6.0)
		assertGlobalError(t, r.Error)
		pqe.Release()
		assertCurCost(t, 0.0, pqe.global)
	})
}

func TestPerQueryEnforcer_State(t *testing.T) {
	pqe := newTestPerQueryEnforcer(10.0, 5.0)
	pqe.Add(15.0)

	r, l := pqe.State()
	assert.Equal(t, cost.Cost(15.0), r.Cost)
	assert.EqualError(t, r.Error, "15 exceeds limit of 5")
	assert.Equal(t, cost.Limit{Threshold: 5.0, Enabled: true}, l)
}

// utils

func newTestEnforcer(limit cost.Limit) *cost.Enforcer {
	return cost.NewEnforcer(
		cost.NewStaticLimitManager(cost.NewLimitManagerOptions().SetDefaultLimit(limit)),
		cost.NewTracker(),
		nil,
	)
}

func newTestPerQueryEnforcer(globalLimit float64, localLimit float64) *perQueryEnforcer {
	return NewPerQueryEnforcerFactory(
		newTestEnforcer(cost.Limit{Threshold: cost.Cost(globalLimit), Enabled: true}),
		newTestEnforcer(cost.Limit{Threshold: cost.Cost(localLimit), Enabled: true}), nil).New().(*perQueryEnforcer)
}

func assertCurCost(t *testing.T, expectedCost cost.Cost, ef cost.EnforcerIF) {
	actual, _ := ef.State()
	assert.Equal(t, cost.Report{
		Cost:  expectedCost,
		Error: nil,
	}, actual)
}

func requireGauge(t *testing.T, gauges map[string]tally.GaugeSnapshot, key string) tally.GaugeSnapshot {
	g, ok := gauges[key]
	require.True(t, ok, "No such gauge: %s. Gauges: %+v", key, gauges)
	return g
}

func requireHistogram(t *testing.T, counters map[string]tally.HistogramSnapshot, key string) tally.HistogramSnapshot {
	c, ok := counters[key]
	require.True(t, ok, "No such counter: %s. Counters: %+v", key, counters)
	return c
}
