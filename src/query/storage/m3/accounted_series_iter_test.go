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

package m3

import (
	"errors"
	"testing"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/x/cost"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEnforcer(limit cost.Cost) *cost.Enforcer {
	limitObj := cost.Limit{Threshold: limit, Enabled: true}
	return cost.NewEnforcer(
		cost.NewStaticLimitManager(cost.NewLimitManagerOptions().SetDefaultLimit(limitObj)),

		cost.NewTracker(),
		nil,
	)
}

func newSimpleLimit(c cost.Cost) cost.Limit {
	return cost.Limit{
		Threshold: c,
		Enabled:   true,
	}
}

// copied from query/cost ; factor out if needed.
func assertCurCost(t *testing.T, expectedCost cost.Cost, ef cost.EnforcerIF) {
	actual, _ := ef.State()
	assert.Equal(t, cost.Report{
		Cost:  expectedCost,
		Error: nil,
	}, actual)
}

type accountedSeriesIterSetup struct {
	Ctrl     *gomock.Controller
	Enforcer *cost.Enforcer
	Iter     *AccountedSeriesIter
}

func setupAccountedSeriesIter(t *testing.T, numValues int, limit cost.Cost) *accountedSeriesIterSetup {
	ctrl := gomock.NewController(t)
	enforcer := newTestEnforcer(limit)

	mockWrappedIter := seriesiter.NewMockSeriesIterator(ctrl, seriesiter.NewMockValidTagGenerator(ctrl), numValues)
	return &accountedSeriesIterSetup{
		Ctrl:     ctrl,
		Enforcer: enforcer,
		Iter:     NewAccountedSeriesIter(mockWrappedIter, enforcer),
	}
}

func TestAccountedSeriesIter_Next(t *testing.T) {
	t.Run("adds to enforcer", func(t *testing.T) {
		setup := setupAccountedSeriesIter(t, 5, 5)
		setup.Iter.Next()
		assertCurCost(t, 1, setup.Enforcer)
	})

	t.Run("returns all values", func(t *testing.T) {
		setup := setupAccountedSeriesIter(t, 5, 6)

		values := make([]ts.Datapoint, 0)
		require.Len(t, values, 0) // I don't trust myself :D
		for setup.Iter.Next() {
			d, _, _ := setup.Iter.Current()
			values = append(values, d)
		}

		assert.NoError(t, setup.Iter.Err())
		assert.Len(t, values, 5)
		for _, d := range values {
			assert.NotEmpty(t, d)
		}
	})

	t.Run("sets error on enforcer error", func(t *testing.T) {
		setup := setupAccountedSeriesIter(t, 5, 2)

		iter := setup.Iter
		iter.Next()
		require.NoError(t, iter.Err())

		iter.Next()
		require.EqualError(t, iter.Err(), "2 exceeds limit of 2")
	})

	t.Run("returns false after enforcer error", func(t *testing.T) {
		setup := setupAccountedSeriesIter(t, 5, 2)
		iter := setup.Iter

		iter.Next()
		iter.Next()

		require.EqualError(t, iter.Err(), "2 exceeds limit of 2")

		assert.False(t, iter.Next())
		assert.True(t, iter.SeriesIterator.Next())
	})

	t.Run("delegates on wrapped error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockIter := mockSeriesIterWithErr(ctrl)
		iter := NewAccountedSeriesIter(mockIter, newTestEnforcer(5))

		assert.True(t, iter.Next(), "the wrapped iterator returns true, so the AcccountedSeriesIterator should return true")
	})
}

func mockSeriesIterWithErr(ctrl *gomock.Controller) *encoding.MockSeriesIterator {
	mockIter := encoding.NewMockSeriesIterator(ctrl)
	mockIter.EXPECT().Err().Return(errors.New("test error"))
	return seriesiter.NewMockSeriesIteratorFromBase(mockIter, seriesiter.NewMockValidTagGenerator(ctrl), 5)
}

func TestAccountedSeriesIter_Err(t *testing.T) {
	t.Run("returns wrapped error over enforcer error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		iter := NewAccountedSeriesIter(mockSeriesIterWithErr(ctrl), newTestEnforcer(1))
		iter.Next()
		assert.EqualError(t, iter.Err(), "test error")
	})

	t.Run("returns enforcer error", func(t *testing.T) {
		setup := setupAccountedSeriesIter(t, 3, 1)
		setup.Iter.Next()
		assert.EqualError(t, setup.Iter.Err(), "1 exceeds limit of 1")
	})
}
