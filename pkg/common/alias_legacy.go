//go:build !pd_master

package common

import "github.com/pingcap/tidb/pkg/parser/model"

type CIStr = model.CIStr

func NewCIStr(s string) CIStr {
	return model.NewCIStr(s)
}
