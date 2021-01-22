package pipeline_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/services/pipeline/mocks"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/utils"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"
)

func Test_PipelineRunner_ExecuteTaskRuns(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	btcUSDPairing := utils.MustUnmarshalToMap(`{"data":{"coin":"BTC","market":"USD"}}`)

	// 1. Setup bridge
	s1 := httptest.NewServer(fakePriceResponder(t, btcUSDPairing, decimal.NewFromInt(9700)))
	defer s1.Close()

	bridgeFeedURL, err := url.ParseRequestURI(s1.URL)
	require.NoError(t, err)
	bridgeFeedWebURL := (*models.WebURL)(bridgeFeedURL)

	_, bridge := cltest.NewBridgeType(t, "example-bridge")
	bridge.URL = *bridgeFeedWebURL
	require.NoError(t, store.ORM.DB.Create(&bridge).Error)

	// 2. Setup success HTTP
	s2 := httptest.NewServer(fakePriceResponder(t, btcUSDPairing, decimal.NewFromInt(9600)))
	defer s2.Close()

	orm := new(mocks.ORM)

	r := pipeline.NewRunner(orm, store.Config)

	spec := pipeline.Spec{}

	// TODO: Add multiple paths
	taskRuns := []pipeline.TaskRun{
		// 1. Bridge request, succeeds
		pipeline.TaskRun{
			ID: 10,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          0,
				DotID:       `ds1`,
				Type:        "bridge",
				JSON:        cltest.MustNewJSONSerializable(t, `{"name": "example-bridge", "Timeout": 0, "requestData": {"data": {"coin": "BTC", "market": "USD"}}}`),
				SuccessorID: null.IntFrom(1),
			},
		},
		pipeline.TaskRun{
			ID: 11,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          1,
				DotID:       `ds1_parse`,
				Type:        "jsonparse",
				JSON:        cltest.MustNewJSONSerializable(t, `{"Lax": false, "path": ["data", "result"], "Timeout": 0}`),
				SuccessorID: null.IntFrom(2),
			},
		},
		pipeline.TaskRun{
			ID: 12,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          2,
				DotID:       `ds1_multiply`,
				Type:        "multiply",
				JSON:        cltest.MustNewJSONSerializable(t, `{"times": "1000000000000000000", "Timeout": 0}`),
				SuccessorID: null.IntFrom(102),
			},
		},
		// 2. HTTP request, succeeds
		pipeline.TaskRun{
			ID: 21,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          33,
				DotID:       `ds2`,
				Type:        "http",
				JSON:        cltest.MustNewJSONSerializable(t, fmt.Sprintf(`{"method": "GET", "url": "%s", "requestData": {"data": {"coin": "BTC", "market": "USD"}}}`, s2.URL)),
				SuccessorID: null.IntFrom(32),
			},
		},
		pipeline.TaskRun{
			ID: 22,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          32,
				DotID:       `ds2_parse`,
				Type:        "jsonparse",
				JSON:        cltest.MustNewJSONSerializable(t, `{"Lax": false, "path": ["data", "result"], "Timeout": 0}`),
				SuccessorID: null.IntFrom(31),
			},
		},
		pipeline.TaskRun{
			ID: 23,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          31,
				DotID:       `ds2_multiply`,
				Type:        "multiply",
				JSON:        cltest.MustNewJSONSerializable(t, `{"times": "1000000000000000000", "Timeout": 0}`),
				SuccessorID: null.IntFrom(102),
			},
		},
		// 3. HTTP request, fails
		// MEDIAN
		pipeline.TaskRun{
			ID: 30,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          102,
				DotID:       `median`,
				Type:        "median",
				JSON:        cltest.MustNewJSONSerializable(t, `{}`),
				SuccessorID: null.IntFrom(203),
			},
		},
		pipeline.TaskRun{
			ID: 13,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          203,
				DotID:       `__result__`,
				Type:        "result",
				JSON:        cltest.MustNewJSONSerializable(t, `{}`),
				SuccessorID: null.Int{},
			},
		},
	}

	run := pipeline.Run{
		PipelineSpec:     spec,
		PipelineTaskRuns: taskRuns,
	}

	result := r.ExecuteTaskRuns(context.Background(), store.DB, run)

	require.Len(t, result.Value, 1)
	fmt.Printf("result %#v\n", result)
	finalValues := result.Value.([]interface{})
	finalValue := finalValues[0].(decimal.Decimal)
	require.Equal(t, "9650000000000000000000", finalValue.String())

	require.Len(t, result.Error, 1)
	finalError := result.Error.(pipeline.FinalErrors)
	require.False(t, finalError.HasErrors())
}
