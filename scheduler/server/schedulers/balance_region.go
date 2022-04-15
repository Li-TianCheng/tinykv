// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	if len(suitableStores) <= 1 {
		return nil
	}
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	var regionInfo *core.RegionInfo
	var src *core.StoreInfo
	for _, store := range suitableStores {
		src = store
		cluster.GetPendingRegionsWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(regions core.RegionsContainer) {
			regionInfo = regions.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
	}
	if regionInfo == nil {
		return nil
	}
	storeIds := regionInfo.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	var dst *core.StoreInfo
	for _, store := range suitableStores {
		if _, ok := storeIds[store.GetID()]; !ok {
			dst = store
		}
	}
	if dst == nil {
		return nil
	}
	if src.GetRegionSize()-dst.GetRegionSize() <= 2*regionInfo.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(dst.GetID())
	if err != nil {
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance-region", cluster,
		regionInfo, operator.OpBalance, src.GetID(), dst.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
