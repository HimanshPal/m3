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
	"fmt"

	"github.com/uber-go/tally"
)

const (
	defaultCostExceededErrorFmt = "%s exceeds limit of %s"
	customCostExceededErrorFmt  = "%s exceeds limit of %s: %s"
)

var (
	noopManager = NewStaticLimitManager(
		NewLimitManagerOptions().
			SetDefaultLimit(Limit{
				Threshold: MaxCost,
				Enabled:   false,
			},
			),
	)
	noopEnforcer = NewEnforcer(noopManager, NewNoopTracker(), nil)
)

// Report is a report on the cost limits of an Enforcer.
type Report struct {
	Cost
	Error error
}

// EnforcerIF instances enforce cost limits for operations.
type EnforcerIF interface {
	Add(op Cost) Report
	State() (Report, Limit)
	Clone() EnforcerIF
}

// Enforcer enforces cost limits for operations.
type Enforcer struct {
	LimitManager
	tracker Tracker

	costMsg string
	metrics EnforcerReporter
}

// NewEnforcer returns a new enforcer for cost limits.
func NewEnforcer(m LimitManager, t Tracker, opts EnforcerOptions) *Enforcer {
	if opts == nil {
		opts = NewEnforcerOptions()
	}

	reporter := opts.Reporter()
	if reporter == nil {
		reporter = newEnforcerMetrics(opts.InstrumentOptions().MetricsScope(), opts.ValueBuckets())
	}

	return &Enforcer{
		LimitManager: m,
		tracker:      t,
		costMsg:      opts.CostExceededMessage(),
		metrics:      reporter,
	}
}

// Add adds the cost of an operation to the enforcer's current total. If the operation exceeds
// the enforcer's limit the enforcer will return a CostLimit error in addition to the new total.
func (e *Enforcer) Add(cost Cost) Report {
	e.metrics.ReportCost(cost)
	current := e.tracker.Add(cost)
	e.metrics.ReportCurrent(current)

	return Report{
		Cost:  current,
		Error: e.checkLimit(current, e.Limit()),
	}
}

// State returns the current state of the enforcer.
func (e *Enforcer) State() (Report, Limit) {
	cost := e.tracker.Current()
	l := e.Limit()
	err := e.checkLimit(cost, l)
	r := Report{
		Cost:  cost,
		Error: err,
	}
	return r, l
}

// Clone clones the current Enforcer. The new Enforcer uses the same Estimator and LimitManager
// as e buts its Tracker is independent.
func (e *Enforcer) Clone() EnforcerIF {
	return &Enforcer{
		LimitManager: e.LimitManager,
		tracker:      NewTracker(),
		costMsg:      e.costMsg,
		metrics:      e.metrics,
	}
}

func (e *Enforcer) checkLimit(cost Cost, limit Limit) error {
	if !limit.Enabled || cost < limit.Threshold {
		return nil
	}

	// Emit metrics on number of operations that are over the limit even when not enabled.
	e.metrics.ReportOverLimit(limit.Enabled)

	if e.costMsg == "" {
		return defaultCostExceededError(cost, limit)
	}
	return costExceededError(e.costMsg, cost, limit)
}

func defaultCostExceededError(cost Cost, limit Limit) error {
	return fmt.Errorf(
		defaultCostExceededErrorFmt,
		fmt.Sprintf("%v", float64(cost)),
		fmt.Sprintf("%v", float64(limit.Threshold)),
	)
}

func costExceededError(customMessage string, cost Cost, limit Limit) error {
	return fmt.Errorf(
		customCostExceededErrorFmt,
		fmt.Sprintf("%v", float64(cost)),
		fmt.Sprintf("%v", float64(limit.Threshold)),
		customMessage,
	)
}

// NoopEnforcer returns a new Enforcer that always returns a current cost of 0 and
//  is always disabled.
func NoopEnforcer() *Enforcer {
	return noopEnforcer
}

// An EnforcerReporter is a listener for Enforcer events.
type EnforcerReporter interface {

	// ReportCost is called on every call to Enforcer#Add with the added cost
	ReportCost(c Cost)

	// ReportCurrent reports the current total on every call to Enforcer#Add
	ReportCurrent(c Cost)

	// ReportOverLimit is called every time an enforcer goes over its limit. enabled is true if the limit manager
	// says the limit is currently enabled.
	ReportOverLimit(enabled bool)
}

type enforcerMetrics struct {
	overLimit           tally.Counter
	overLimitAndEnabled tally.Counter
}

func (em enforcerMetrics) ReportCurrent(c Cost) {
}

func (em enforcerMetrics) ReportCost(c Cost) {

}

func (em enforcerMetrics) ReportOverLimit(enabled bool) {
	if enabled {
		em.overLimitAndEnabled.Inc(1)
	} else {
		em.overLimit.Inc(1)
	}
}

func newEnforcerMetrics(s tally.Scope, b tally.ValueBuckets) enforcerMetrics {
	return enforcerMetrics{
		overLimit:           s.Counter("over-limit"),
		overLimitAndEnabled: s.Counter("over-limit-and-enabled"),
	}
}
