//go:build pd_master

package mysql

import "github.com/pingcap/tidb/pkg/sessionctx/vardef"

func getDefaultMaxAllowedPacket() int64 {
	return int64(vardef.DefMaxAllowedPacket)
}
