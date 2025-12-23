//go:build pd_master

package factory

import (
	"strings"
	"time"

	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/version"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// PdClient creates new pd client.
func (f *factoryImpl) PdClient() (pd.Client, error) {
	credential := f.GetCredential()
	grpcTLSOption, err := f.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	pdAddr := f.GetPdAddr()
	if len(pdAddr) == 0 {
		return nil, errors.ErrInvalidServerOption.
			GenWithStack("Empty PD address. Please use --pd to specify PD cluster addresses")
	}
	pdEndpoints := strings.Split(pdAddr, ",")
	for _, ep := range pdEndpoints {
		if err = util.VerifyPdEndpoint(ep, credential.IsTLSEnabled()); err != nil {
			return nil, errors.ErrInvalidServerOption.Wrap(err).GenWithStackByArgs()
		}
	}

	pdClient, err := pd.NewClientWithContext(
		f.ctx, "cdc-factory", pdEndpoints, credential.PDSecurityOption(),
		pdopt.WithMaxErrorRetry(maxGetPDClientRetryTimes),
		// TODO(hi-rustin): add gRPC metrics to Options.
		// See also: https://github.com/pingcap/tiflow/pull/2341#discussion_r673032407.
		pdopt.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
	if err != nil {
		return nil, errors.Annotatef(err,
			"Fail to open PD client. Please check the pd address(es)  \"%s\"", pdAddr)
	}

	err = version.CheckClusterVersion(f.ctx, pdClient, pdEndpoints, credential, true)
	if err != nil {
		return nil, err
	}

	return pdClient, nil
}
