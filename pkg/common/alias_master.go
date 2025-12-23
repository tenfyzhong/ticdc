//go:build pd_master

package common

import "github.com/pingcap/tidb/pkg/parser/ast"

type CIStr = ast.CIStr

func NewCIStr(s string) CIStr {
	return ast.NewCIStr(s)
}
