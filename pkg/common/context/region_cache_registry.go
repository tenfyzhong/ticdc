// Copyright 2025 PingCAP, Inc.
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

package context

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// RegionCacheRegistry is a factory for RegionCache
type RegionCacheRegistry struct {
	keyspaceIDRegionCacheMap   sync.Map
	keyspaceNameRegionCacheMap sync.Map
}

func NewRegionCacheRegistry() *RegionCacheRegistry {
	return &RegionCacheRegistry{}
}

// Get returns regionCache for keyspace
func (f *RegionCacheRegistry) Get(keyspaceID uint32) *tikv.RegionCache {
	if regionCache, ok := f.keyspaceIDRegionCacheMap.Load(keyspaceID); ok {
		return regionCache.(*tikv.RegionCache)
	}
	return nil
}

func (f *RegionCacheRegistry) GetByName(keyspace string) *tikv.RegionCache {
	if regionCache, ok := f.keyspaceNameRegionCacheMap.Load(keyspace); ok {
		return regionCache.(*tikv.RegionCache)
	}
	return nil
}

// Register registers regionCache for keyspace
// For classic cdc, the keyspace is alwasy "default", keyspaceID is always 0
func (f *RegionCacheRegistry) Register(keyspace string, keyspaceID uint32, pdClient pd.Client) error {
	if f.Get(keyspaceID) != nil {
		return nil
	}

	var regionCache *tikv.RegionCache
	if kerneltype.IsNextGen() {
		keyspacePdClient, err := tikv.NewCodecPDClientWithKeyspace(tikv.ModeTxn, pdClient, keyspace)
		if err != nil {
			return errors.Trace(err)
		}
		regionCache = tikv.NewRegionCache(keyspacePdClient)
	} else {
		regionCache = tikv.NewRegionCache(pdClient)
	}
	f.keyspaceIDRegionCacheMap.Store(keyspaceID, regionCache)
	f.keyspaceNameRegionCacheMap.Store(keyspace, regionCache)
	return nil
}
