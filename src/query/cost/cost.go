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

	"github.com/m3db/m3/src/x/cost"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// NB (amains) type alias here allows us the same visibility restrictions as defining an interface
// (PerQueryEnforcerOpts isn't directly constructable), but without the unnecessary boilerplate.

// PerQueryEnforcerOpts configures a PerQueryEnforcer.
type PerQueryEnforcerOpts struct {
	valueBuckets   tally.Buckets
	instrumentOpts instrument.Options
}

// NewPerQueryEnforcerOpts constructs a default PerQueryEnforcerOpts
func NewPerQueryEnforcerOpts(instrumentOps instrument.Options) *PerQueryEnforcerOpts {
	return &PerQueryEnforcerOpts{
		instrumentOpts: instrumentOps,
		valueBuckets:   tally.MustMakeExponentialValueBuckets(1, 2, 20),
	}
}

// SetDatapointsDistroBuckets -- see DatapointsDistroBuckets
func (pe PerQueryEnforcerOpts) SetDatapointsDistroBuckets(b tally.Buckets) *PerQueryEnforcerOpts {
	pe.valueBuckets = b
	return &pe
}

// DatapointsDistroBuckets is the histogram bucket config for the per-query datapoint histogram.
func (pe *PerQueryEnforcerOpts) DatapointsDistroBuckets() tally.Buckets {
	return pe.valueBuckets
}

// perQueryEnforcerFactory constructs PerQueryEnforcer instances with a shared global enforcer and independent
// local enforcers.
type perQueryEnforcerFactory struct {
	global *cost.Enforcer
	local  *cost.Enforcer
	opts   *PerQueryEnforcerOpts
}

// A PerQueryEnforcerFactory constructs PerQueryEnforcer instances using a shared global enforcer.
type PerQueryEnforcerFactory interface {
	// GlobalEnforcer is the enforcer shared across instances.
	GlobalEnforcer() *cost.Enforcer

	// New constructs a new PerQueryEnforcer sharing the global enforcer but with a clean local tracker.
	New() PerQueryEnforcer
}

// NewPerQueryEnforcerFactory constructs a perQueryEnforcerFactory which will use the provided enforcers and options
// to construct PerQueryEnforcer instances. The parent enforcer will be shared between instances; the
// provided localEnforcer will be cloned for each call to New().
func NewPerQueryEnforcerFactory(parent *cost.Enforcer, localEnforcer *cost.Enforcer, opts *PerQueryEnforcerOpts) PerQueryEnforcerFactory {
	if opts == nil {
		opts = NewPerQueryEnforcerOpts(instrument.NewOptions())
	}

	return &perQueryEnforcerFactory{
		global: parent,
		local:  localEnforcer,
		opts:   opts,
	}
}

// New constructs a new PerQueryEnforcer using this factory's configuration. The parent enforcer will be shared between
// instances; the provided localEnforcer will be cloned for each call to New().
func (pef *perQueryEnforcerFactory) New() PerQueryEnforcer {
	scope := pef.opts.instrumentOpts.MetricsScope()

	return &perQueryEnforcer{
		// important: clone the local enforcer to ensure per query isolation
		local:  pef.local.Clone(),
		global: pef.global,

		globalCurrentDatapoints:  scope.SubScope("global").Gauge("datapoints"),
		perQueryDatapointsDistro: scope.SubScope("per-query").Histogram("datapoints-distro", pef.opts.DatapointsDistroBuckets()),
	}
}

var noopEnforcerFactory = NewPerQueryEnforcerFactory(cost.NoopEnforcer(), cost.NoopEnforcer(), nil)

// NoopPerQueryEnforcerFactory returns a perQueryEnforcerFactory which only generates noop enforcer instances.
func NoopPerQueryEnforcerFactory() PerQueryEnforcerFactory {
	return noopEnforcerFactory
}

// GlobalEnforcer returns the global enforcer instance for this factory.
func (pef *perQueryEnforcerFactory) GlobalEnforcer() *cost.Enforcer {
	return pef.global
}

// PerQueryEnforcer is a cost.EnforcerIF implementation which tracks resource usage both at a per-query and a global
// level.
type PerQueryEnforcer interface {
	cost.EnforcerIF

	Report()
	Release()
}

// PerQueryEnforcer wraps around two cost.EnforcerIF instances to enforce limits at both the local (per query)
// and the global (across all queries) levels.
type perQueryEnforcer struct {
	local  cost.EnforcerIF
	global cost.EnforcerIF
	scope  tally.Scope

	globalCurrentDatapoints  tally.Gauge
	perQueryDatapointsDistro tally.Histogram
}

// Add adds the provided cost to both the global and local enforcers. The returned report will have Error set
// if either local or global errored. In case of no error, the local report is returned.
func (se *perQueryEnforcer) Add(c cost.Cost) cost.Report {
	// TODO: do we need a lock over both of these? Maybe; addition of cost isn't atomic as of now (though both local
	// and global should be safe individually, fwiw)

	localR := se.local.Add(c)
	globalR := se.global.Add(c)

	// check our local limit first
	if localR.Error != nil {
		return cost.Report{
			Cost:  localR.Cost,
			Error: fmt.Errorf("exceeded per query limit: %s", localR.Error.Error()),
		}
	}

	// check the global limit
	if globalR.Error != nil {
		return cost.Report{
			Error: fmt.Errorf("exceeded global limit: %s", globalR.Error.Error()),
			Cost:  globalR.Cost,
		}
	}

	return localR
}

// Report sends stats on the current state of this PerQueryEnforcer using the provided tally.Scope.
func (se *perQueryEnforcer) Report() {
	globalR, _ := se.global.State()
	se.globalCurrentDatapoints.Update(float64(globalR.Cost))

	localR, _ := se.local.State()
	se.perQueryDatapointsDistro.RecordValue(float64(localR.Cost))
}

// State returns the per-query state of the enforcer.
func (se *perQueryEnforcer) State() (cost.Report, cost.Limit) {
	return se.local.State()
}

// Release releases all resources tracked by this enforcer back to the global enforcer
func (se *perQueryEnforcer) Release() {
	r, _ := se.local.State()
	se.global.Add(-r.Cost)
}
