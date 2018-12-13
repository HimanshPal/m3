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

const (
	BlockLevel  = "block"
	QueryLevel  = "query"
	GlobalLevel = "global"
)

// NB (amains) type alias here allows us the same visibility restrictions as defining an interface
// (PerQueryEnforcerOpts isn't directly constructable), but without the unnecessary boilerplate.

// PerQueryEnforcerOpts configures a PerQueryEnforcer.
type PerQueryEnforcerOpts struct {
	valueBuckets   tally.Buckets
	instrumentOpts instrument.Options
	childFactory   childFactoryFn
}

// NewPerQueryEnforcerOpts constructs a default PerQueryEnforcerOpts
func NewPerQueryEnforcerOpts() *PerQueryEnforcerOpts {
	return &PerQueryEnforcerOpts{}
}

func (pe PerQueryEnforcerOpts) SetChildFactory(f childFactoryFn) *PerQueryEnforcerOpts {
	pe.childFactory = f
	return &pe
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

// PerQueryEnforcer is a cost.EnforcerIF implementation which tracks resource usage both at a per-query and a global
// level.
type PerQueryEnforcer interface {
	cost.EnforcerIF

	Child(resourceName string) PerQueryEnforcer
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

type childFactoryFn func(resourceName string, parent *ChainedEnforcer) *ChainedEnforcer

type ChainedEnforcer struct {
	resourceName string
	local        cost.EnforcerIF
	parent       cost.EnforcerIF
	childFactory childFactoryFn
}

func NoopChainedEnforcer() *ChainedEnforcer {
	return NewRootChainedEnforcer("", cost.NoopEnforcer(), nil)
}

func NewRootChainedEnforcer(resourceName string, root *cost.Enforcer, opts *PerQueryEnforcerOpts) *ChainedEnforcer {
	return NewChainedEnforcer(resourceName, nil, root, opts)
}

func NewChainedEnforcer(resourceName string, parent cost.EnforcerIF, local cost.EnforcerIF, opts *PerQueryEnforcerOpts) *ChainedEnforcer {
	if opts == nil {
		opts = NewPerQueryEnforcerOpts()
	}
	return &ChainedEnforcer{
		resourceName: resourceName,
		local:        local,
		parent:       parent,
		childFactory: opts.childFactory,
	}
}

func (ce *ChainedEnforcer) Add(c cost.Cost) cost.Report {
	if ce.parent == nil {
		return ce.wrapLocalResult(ce.local.Add(c))
	}

	localR := ce.local.Add(c)
	globalR := ce.parent.Add(c)

	// check our local limit first
	if localR.Error != nil {
		return ce.wrapLocalResult(localR)
	}

	// check the global limit
	if globalR.Error != nil {
		return globalR
	}

	return localR
}

func (ce *ChainedEnforcer) wrapLocalResult(localR cost.Report) cost.Report {
	if localR.Error != nil {
		return cost.Report{
			Cost:  localR.Cost,
			Error: fmt.Errorf("exceeded %s limit: %s", ce.resourceName, localR.Error.Error()),
		}
	}
	return localR
}

func (ce *ChainedEnforcer) Child(resourceName string) PerQueryEnforcer {
	if ce.childFactory == nil {
		return &ChainedEnforcer{
			resourceName: resourceName,
			parent:       ce,
			local:        ce.local.Clone(),
		}
	}

	return ce.childFactory(resourceName, ce)
}

func (ce *ChainedEnforcer) Clone() cost.EnforcerIF {
	return ce
}

func (ce *ChainedEnforcer) State() (cost.Report, cost.Limit) {
	return ce.local.State()
}

// Release releases all resources tracked by this enforcer back to the global enforcer
func (ce *ChainedEnforcer) Release() {
	r, _ := ce.local.State()
	ce.Add(-r.Cost)
}
