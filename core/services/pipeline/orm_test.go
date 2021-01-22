package pipeline_test

import (
	"context"
	"testing"

	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/services/postgres"
	"github.com/stretchr/testify/require"
)

func Test_PipelineORM_CreateRun(t *testing.T) {
	store, cleanup := cltest.NewStore(t)
	defer cleanup()
	db := store.DB

	job := cltest.MustInsertSampleDirectRequestJob(t, db)

	eventBroadcaster := postgres.NewEventBroadcaster(store.Config.DatabaseURL(), 0, 0)

	orm := pipeline.NewORM(db, store.Config, eventBroadcaster)

	meta := make(map[string]interface{})

	runID, err := orm.CreateRun(context.Background(), job.ID, meta)
	require.NoError(t, err)

	// Check that JobRun, TaskRuns were created

	var prs []pipeline.Run
	var trs []pipeline.TaskRun

	require.NoError(t, db.Find(&prs).Error)
	require.NoError(t, db.Find(&trs).Error)

	require.Len(t, prs, 1)
	require.Equal(t, runID, prs[0].ID)
	require.Len(t, trs, 4)
}
