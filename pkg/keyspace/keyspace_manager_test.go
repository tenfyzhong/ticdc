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

package keyspace

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func Test_manager_update(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := pdutil.NewMockPDAPIClient(ctrl)

	appcontext.SetService(appcontext.PDAPIClient, mockClient)

	const keyspace = "ks1"
	mu := &sync.Mutex{}

	// step 1, load a metadata to make the map has a member
	meta1 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 1,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta1, nil).Times(1)

	m := &manager{
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}
	m.forceLoadKeyspace(context.Background(), keyspace)
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta1,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta1,
	}, m.keyspaceIDMap)

	// step 2, lock success
	meta2 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 2,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta2, nil).Times(1)
	m.update(mu)
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta2,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta2,
	}, m.keyspaceIDMap)

	// step 3, lock failed
	mu.Lock()
	m.update(mu)
	mu.Unlock()
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta2,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta2,
	}, m.keyspaceIDMap)

	// step 4, lock success again
	meta3 := &keyspacepb.KeyspaceMeta{
		Id:             1,
		Name:           keyspace,
		State:          0,
		CreatedAt:      1,
		StateChangedAt: 3,
		Config:         map[string]string{},
	}
	mockClient.EXPECT().LoadKeyspace(gomock.Any(), gomock.Eq(keyspace)).Return(meta3, nil).Times(1)
	m.update(mu)
	require.EqualValues(t, map[string]*keyspacepb.KeyspaceMeta{
		"ks1": meta3,
	}, m.keyspaceMap)
	require.EqualValues(t, map[uint32]*keyspacepb.KeyspaceMeta{
		uint32(1): meta3,
	}, m.keyspaceIDMap)
}
