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

	"github.com/m3db/m3/_tools/src/github.com/fossas/fossa-cli/errors"
	"github.com/m3db/m3/src/x/cost"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

const (
	BlockLevel  = "block"
	QueryLevel  = "query"
	GlobalLevel = "global"
)

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

// PerQueryEnforcer is a cost.EnforcerIF implementation which tracks resource usage both at a per-query and a global
// level.
type PerQueryEnforcer interface {
	cost.EnforcerIF

	Child(resourceName string) PerQueryEnforcer
	Release()
}

type childFactoryFn func(resourceName string, parent *ChainedEnforcer) *ChainedEnforcer

type ChainedEnforcer struct {
	resourceName string
	local        cost.EnforcerIF
	parent       cost.EnforcerIF
	models       []cost.EnforcerIF
}

var noopChainedEnforcer, _ = NewChainedEnforcerFromModels("", []cost.EnforcerIF{cost.NoopEnforcer()})

func NoopPerQueryEnforcerFactory() *ChainedEnforcer {
	return NoopChainedEnforcer()
}

func NoopChainedEnforcer() *ChainedEnforcer {
	return noopChainedEnforcer
}

func NewChainedEnforcerFromModels(rootResourceName string, models []cost.EnforcerIF) (*ChainedEnforcer, error) {
	if len(models) == 0 {
		return nil, errors.New("must provide at least one EnforcerIF instance for a ChainedEnforcer")
	}

	return &ChainedEnforcer{
		resourceName: rootResourceName,
		parent:       nil, // root has nil parent
		local:        models[0],
		models:       models[1:],
	}, nil
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
	// no more models; just return a noop default. TODO: this could be a panic case? Technically speaking it's
	// misconfiguration.
	if len(ce.models) == 0 {
		return NoopChainedEnforcer()
	}

	return &ChainedEnforcer{
		resourceName: resourceName,
		parent:       ce,
		local:        ce.models[0].Clone(),
		models:       ce.models[1:],
	}
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
