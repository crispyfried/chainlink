package pipeline_test

import (
	"context"
	"testing"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/services/pipeline/mocks"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"
)

func Test_PipelineRunner_ExecuteTaskRuns(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()

	orm := new(mocks.ORM)

	r := pipeline.NewRunner(orm, store.Config)

	spec := pipeline.Spec{}

	taskRuns := []pipeline.TaskRun{
		pipeline.TaskRun{
			ID: 1,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          0,
				DotID:       `ds`,
				Type:        "bridge",
				JSON:        cltest.MustNewJSONSerializable(t, `{"name": "bridge-nomics", "Timeout": 0, "requestData": {"data": {"to": "ETH", "from": "DMG"}}}`),
				SuccessorID: null.IntFrom(2),
			},
		},
		pipeline.TaskRun{
			ID: 2,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          1,
				DotID:       `ds_parse`,
				Type:        "jsonparse",
				JSON:        cltest.MustNewJSONSerializable(t, `{"Lax": false, "path": ["result"], "Timeout": 0}`),
				SuccessorID: null.IntFrom(3),
			},
		},
		pipeline.TaskRun{
			ID: 3,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          2,
				DotID:       `ds_multiply`,
				Type:        "multiply",
				JSON:        cltest.MustNewJSONSerializable(t, `{"times": "1000000000000000000", "Timeout": 0}`),
				SuccessorID: null.IntFrom(4),
			},
		},
		pipeline.TaskRun{
			ID: 4,
			PipelineTaskSpec: pipeline.TaskSpec{
				ID:          3,
				DotID:       `__result__`,
				Type:        "result",
				JSON:        cltest.MustNewJSONSerializable(t, `{"Timeout": 0}`),
				SuccessorID: null.Int{},
			},
		},
	}

	run := pipeline.Run{
		PipelineSpec:     spec,
		PipelineTaskRuns: taskRuns,
	}

	result := r.ExecuteTaskRuns(context.Background(), store.DB, run)

	require.Equal(t, "foo", result)
}
