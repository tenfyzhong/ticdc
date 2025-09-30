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

package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractChangefeedKeySuffix(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name             string
		args             args
		wantKs           string
		wantCf           string
		wantIsStatus     bool
		wantIsChangefeed bool
	}{
		{
			name: "an empty key",
			args: args{
				key: "",
			},
			wantIsChangefeed: false,
		},
		{
			name: "an invalid key",
			args: args{
				key: "foobar",
			},
			wantIsChangefeed: false,
		},
		{
			name: "an slash key",
			args: args{
				key: "/",
			},
			wantIsChangefeed: false,
		},
		{
			name: "3 parts",
			args: args{
				key: "/tidb/cdc/default",
			},
			wantIsChangefeed: false,
		},
		{
			name: "not a changefeed",
			args: args{
				key: "/tidb/cdc/default/keyspace1/foobar/info/hello",
			},
			wantIsChangefeed: false,
		},
		{
			name: "a changefeed info",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/info/hello",
			},
			wantKs:           "keyspace1",
			wantCf:           "hello",
			wantIsStatus:     false,
			wantIsChangefeed: true,
		},
		{
			name: "a changefeed status",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/status/hello",
			},
			wantKs:           "keyspace1",
			wantCf:           "hello",
			wantIsStatus:     true,
			wantIsChangefeed: true,
		},
		{
			name: "an invalid changefeed status",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/status",
			},
			wantIsChangefeed: false,
		},
		{
			name: "capture info",
			args: args{
				key: "/tidb/cdc/default/__cdc_meta__/capture/786afb7b-c780-48df-8fb6-567d4647c007",
			},
			wantIsChangefeed: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKs, gotCf, gotIsStatus, gotIsChangefeed := extractChangefeedKeySuffix(tt.args.key)
			require.Equal(t, tt.wantKs, gotKs)
			require.Equal(t, tt.wantCf, gotCf)
			require.Equal(t, tt.wantIsStatus, gotIsStatus)
			require.Equal(t, tt.wantIsChangefeed, gotIsChangefeed)
		})
	}
}

// func TestCDCEtcdClientImpl_GetChangefeedInfoAndStatus(t *testing.T) {
// 	type fields struct {
// 		Client        Client
// 		ClusterID     string
// 		etcdClusterID uint64
// 	}
// 	type args struct {
// 		ctx context.Context
// 	}
// 	tests := []struct {
// 		name          string
// 		fields        func(ctx context.Context, ctrl *gomock.Controller) fields
// 		args          args
// 		wantRevision  int64
// 		wantStatusMap map[common.ChangeFeedDisplayName]*mvccpb.KeyValue
// 		wantInfoMap   map[common.ChangeFeedDisplayName]*mvccpb.KeyValue
// 		assertion     require.ErrorAssertionFunc
// 	}{
// 		{
// 			name: "get changefeeds failed",
// 			fields: func(ctx context.Context, ctrl *gomock.Controller) fields {
// 				client := mock_etcd.NewMockClient(ctrl)
// 				client.EXPECT().Get(ctx, "/tidb/cdc/cluster-id", clientv3.WithPrefix()).Return(nil, errors.New("etcd failed")).Times(1)
// 				return fields{
// 					Client:        mock_etcd.NewMockClient(ctrl),
// 					ClusterID:     "cluster-id",
// 					etcdClusterID: uint64(1),
// 				}
// 			},
// 			args: args{
// 				ctx: context.Background(),
// 			},
// 			wantRevision:  int64(0),
// 			wantStatusMap: nil,
// 			wantInfoMap:   nil,
// 			assertion: func(t require.TestingT, err error, opts ...any) {
// 				require.ErrorContains(t, err, "etcd failed")
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			ctrl := gomock.NewController(t)
// 			defer ctrl.Finish()
//
// 			fields := tt.fields(tt.args.ctx, ctrl)
// 			c := &CDCEtcdClientImpl{
// 				Client:        fields.Client,
// 				ClusterID:     fields.ClusterID,
// 				etcdClusterID: fields.etcdClusterID,
// 			}
//
// 			gotRevision, gotStatusMap, gotInfoMap, err := c.GetChangefeedInfoAndStatus(tt.args.ctx)
// 			tt.assertion(t, err)
// 			require.Equal(t, tt.wantRevision, gotRevision)
// 			require.Equal(t, tt.wantStatusMap, gotStatusMap)
// 			require.Equal(t, tt.wantInfoMap, gotInfoMap)
// 		})
// 	}
// }
