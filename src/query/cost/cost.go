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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/cost"
)

const (
	// BlockLevel identifies per-block enforcers
	BlockLevel = "block"
	// QueryLevel identifies per-query enforcers
	QueryLevel = "query"

	// GlobalLevel identifies global enforcers.
	GlobalLevel = "global"
)

// PerQueryEnforcer is a cost.EnforcerIF implementation which tracks resource usage both at a per-query and a global
// level.
type PerQueryEnforcer interface {
	cost.EnforcerIF

	Child(resourceName string) PerQueryEnforcer
	Release()
}

// ChainedEnforcer implements cost.EnforcerIF to enforce limits on multiple resources at once, linked together in a tree.
// Child() creates a new ChainedEnforcer which rolls up into this one.
type ChainedEnforcer struct {
	resourceName string
	local        cost.EnforcerIF
	parent       cost.EnforcerIF
	models       []cost.EnforcerIF
}

var noopChainedEnforcer, _ = NewChainedEnforcer("", []cost.EnforcerIF{cost.NoopEnforcer()})

// NoopChainedEnforcer returns a ChainedEnforcer which enforces no limits and does no reporting.
func NoopChainedEnforcer() *ChainedEnforcer {
	return noopChainedEnforcer
}

// NewChainedEnforcer constructs a ChainedEnforcer which creates children using the provided models.
// models[0] enforces this instance; models[1] enforces the first level of children, and so on.
func NewChainedEnforcer(rootResourceName string, models []cost.EnforcerIF) (*ChainedEnforcer, error) {
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

// Add adds the given cost both to this enforcer and any parents, working recursively until the root is reached.
// The most local error is preferred.
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

// Child creates a new ChainedEnforcer whose resource consumption rolls up into this instance.
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

// Clone on a ChainedEnforcer is a noop--TODO: implement?
func (ce *ChainedEnforcer) Clone() cost.EnforcerIF {
	return ce
}

// State returns the local state of this enforcer (ignoring anything further up the chain).
func (ce *ChainedEnforcer) State() (cost.Report, cost.Limit) {
	return ce.local.State()
}

// Release releases all resources tracked by this enforcer back to the global enforcer
func (ce *ChainedEnforcer) Release() {
	r, _ := ce.local.State()
	ce.Add(-r.Cost)
}
