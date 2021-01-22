package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/smartcontractkit/chainlink/core/store/models"

	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/postgres"
	"github.com/smartcontractkit/chainlink/core/utils"
)

type (
	// Runner checks the DB for incomplete TaskRuns and runs them.  For a
	// TaskRun to be eligible to be run, its parent/input tasks must already
	// all be complete.
	Runner interface {
		Start() error
		Close() error
		CreateRun(ctx context.Context, jobID int32, meta map[string]interface{}) (int64, error)
		AwaitRun(ctx context.Context, runID int64) error
		ResultsForRun(ctx context.Context, runID int64) ([]Result, error)
	}

	runner struct {
		orm                             ORM
		config                          Config
		processIncompleteTaskRunsWorker utils.SleeperTask
		runReaperWorker                 utils.SleeperTask

		utils.StartStopOnce
		chStop chan struct{}
		chDone chan struct{}
	}
)

var (
	promPipelineTaskExecutionTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pipeline_task_execution_time",
		Help: "How long each pipeline task took to execute",
	},
		[]string{"pipeline_spec_id", "task_type"},
	)
)

func NewRunner(orm ORM, config Config) *runner {
	r := &runner{
		orm:    orm,
		config: config,
		chStop: make(chan struct{}),
		chDone: make(chan struct{}),
	}
	r.processIncompleteTaskRunsWorker = utils.NewSleeperTask(
		utils.SleeperTaskFuncWorker(r.processUnfinishedRuns),
	)
	r.runReaperWorker = utils.NewSleeperTask(
		utils.SleeperTaskFuncWorker(r.runReaper),
	)
	return r
}

func (r *runner) Start() error {
	if !r.OkayToStart() {
		return errors.New("Pipeline runner has already been started")
	}
	go r.runLoop()
	return nil
}

func (r *runner) Close() error {
	if !r.OkayToStop() {
		return errors.New("Pipeline runner has already been stopped")
	}

	close(r.chStop)
	<-r.chDone

	return nil
}

func (r *runner) destroy() {
	err := r.processIncompleteTaskRunsWorker.Stop()
	if err != nil {
		logger.Error(err)
	}
	err = r.runReaperWorker.Stop()
	if err != nil {
		logger.Error(err)
	}
}

func (r *runner) runLoop() {
	defer close(r.chDone)
	defer r.destroy()

	var newRunEvents <-chan postgres.Event
	newRunsSubscription, err := r.orm.ListenForNewRuns()
	if err != nil {
		logger.Error("Pipeline runner could not subscribe to new run events, falling back to polling")
	} else {
		defer newRunsSubscription.Close()
		newRunEvents = newRunsSubscription.Events()
	}

	dbPollTicker := time.NewTicker(utils.WithJitter(r.config.TriggerFallbackDBPollInterval()))
	defer dbPollTicker.Stop()

	runReaperTicker := time.NewTicker(r.config.JobPipelineReaperInterval())
	defer runReaperTicker.Stop()

	for {
		select {
		case <-r.chStop:
			return
		case <-newRunEvents:
			r.processIncompleteTaskRunsWorker.WakeUp()
		case <-dbPollTicker.C:
			r.processIncompleteTaskRunsWorker.WakeUp()
		case <-runReaperTicker.C:
			r.runReaperWorker.WakeUp()
		}
	}
}

func (r *runner) CreateRun(ctx context.Context, jobID int32, meta map[string]interface{}) (int64, error) {
	runID, err := r.orm.CreateRun(ctx, jobID, meta)
	if err != nil {
		return 0, err
	}
	logger.Infow("Pipeline run created", "jobID", jobID, "runID", runID)
	return runID, nil
}

func (r *runner) AwaitRun(ctx context.Context, runID int64) error {
	ctx, cancel := utils.CombinedContext(r.chStop, ctx)
	defer cancel()
	return r.orm.AwaitRun(ctx, runID)
}

func (r *runner) ResultsForRun(ctx context.Context, runID int64) ([]Result, error) {
	ctx, cancel := utils.CombinedContext(r.chStop, ctx)
	defer cancel()
	return r.orm.ResultsForRun(ctx, runID)
}

// NOTE: This could potentially run on a different machine in the cluster than
// the one that originally added the task runs.
// TODO: Fix naming everywhere
func (r *runner) processUnfinishedRuns() {
	threads := int(r.config.JobPipelineParallelism())

	var wg sync.WaitGroup
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-r.chStop:
					return
				default:
				}

				anyRemaining, err := r.processRun()
				if err != nil {
					logger.Errorf("Error processing incomplete task runs: %v", err)
					return
				} else if !anyRemaining {
					return
				}
			}
		}()
	}
	wg.Wait()
}

func (r *runner) processRun() (anyRemaining bool, err error) {
	ctx, cancel := utils.CombinedContext(r.chStop, r.config.JobPipelineMaxTaskDuration())
	defer cancel()

	return r.orm.ProcessNextUnfinishedRun(ctx, func(ctx context.Context, txdb *gorm.DB, run Run) Result {
		return r.ExecuteTaskRuns(ctx, txdb, run)
	})
}

type memoryTaskRun struct {
	taskRun       TaskRun
	next          *memoryTaskRun
	nPredecessors int
	finished      bool
	inputs        []Result
	predMu        sync.RWMutex
	finishMu      sync.Mutex
}

func (r *runner) ExecuteTaskRuns(ctx context.Context, txdb *gorm.DB, run Run) Result {
	logger.Infow("Initiating tasks for pipeline run", "runID", run.ID)

	// Find "firsts" and work forwards
	// 1. Make map of all memory task runs keyed by task spec id
	all := make(map[int32]*memoryTaskRun)
	for _, tr := range run.PipelineTaskRuns {
		mtr := memoryTaskRun{
			taskRun: tr,
		}
		all[tr.PipelineTaskSpec.ID] = &mtr
	}

	var graph []*memoryTaskRun

	// 2. Fill in predecessor count and next, append firsts to work graph
	for id, mtr := range all {
		for _, pred := range all {
			if !pred.taskRun.PipelineTaskSpec.SuccessorID.IsZero() && pred.taskRun.PipelineTaskSpec.SuccessorID.ValueOrZero() == int64(id) {
				mtr.nPredecessors++
			}
		}

		if mtr.taskRun.PipelineTaskSpec.SuccessorID.IsZero() {
			mtr.next = nil
		} else {
			mtr.next = all[int32(mtr.taskRun.PipelineTaskSpec.SuccessorID.ValueOrZero())]
		}

		if mtr.nPredecessors == 0 {
			// No predecessors so this is the first one
			graph = append(graph, mtr)
		}
	}

	// 3. Execute tasks using "fan in" job processing

	var finalResult Result
	var wg sync.WaitGroup
	wg.Add(len(graph))

	for _, mtr := range graph {
		go func(m *memoryTaskRun) {
			defer wg.Done()
			for m != nil {
				m.predMu.RLock()
				nPredecessors := m.nPredecessors
				inputs := m.inputs
				m.predMu.RUnlock()
				if nPredecessors > 0 {
					// This one is still waiting another chain, abandon this
					// goroutine and let the other handle it
					return
				}

				var finished bool

				// Avoid double execution, only one goroutine may finish the task
				m.finishMu.Lock()
				finished = m.finished
				if finished {
					m.finishMu.Unlock()
					return
				}
				m.finished = true
				m.finishMu.Unlock()

				start := time.Now()

				result := r.executeTaskRun(txdb, m.taskRun, inputs)

				// TODO: As a further optimisation later we can consider pulling out these updates
				if err := txdb.Exec(`UPDATE pipeline_task_runs SET output = ?, error = ?, finished_at = ? WHERE id = ?`, result.OutputsDB(), result.ErrorsDB(), time.Now(), m.taskRun.ID).Error; err != nil {
					logger.Errorw("could not mark pipeline_task_run as finished", "err", err)
				}

				elapsed := time.Since(start)
				promPipelineTaskExecutionTime.WithLabelValues(string(m.taskRun.PipelineTaskSpec.PipelineSpecID), string(m.taskRun.PipelineTaskSpec.Type)).Set(float64(elapsed))

				if m.next == nil {
					finalResult = result
					return
				}

				m.next.predMu.Lock()
				m.next.inputs = append(m.next.inputs, result)
				m.next.nPredecessors--
				m.next.predMu.Unlock()

				m = m.next
			}

		}(mtr)
	}

	wg.Wait()

	logger.Infow("Finished all tasks for pipeline run", "runID", run.ID)

	return finalResult
}

func (r *runner) executeTaskRun(txdb *gorm.DB, taskRun TaskRun, inputs []Result) Result {
	loggerFields := []interface{}{
		"taskName", taskRun.PipelineTaskSpec.DotID,
		"taskID", taskRun.PipelineTaskSpecID,
		"runID", taskRun.PipelineRunID,
		"taskRunID", taskRun.ID,
	}

	task, err := UnmarshalTaskFromMap(
		taskRun.PipelineTaskSpec.Type,
		taskRun.PipelineTaskSpec.JSON.Val,
		taskRun.PipelineTaskSpec.DotID,
		r.config,
		txdb,
	)
	if err != nil {
		logger.Errorw("Pipeline task run could not be unmarshaled", append(loggerFields, "error", err)...)
		return Result{Error: err}
	}

	spec := taskRun.PipelineTaskSpec.PipelineSpec

	// Order of precedence for task timeout:
	// - Specific task timeout (task.TaskTimeout)
	// - Job level task timeout (spec.MaxTaskDuration)
	// - Node level task timeout (JobPipelineMaxTaskDuration)
	taskTimeout, isSet := task.TaskTimeout()
	var ctx context.Context
	if isSet {
		var cancel context.CancelFunc
		ctx, cancel = utils.CombinedContext(r.chStop, taskTimeout)
		defer cancel()
	} else if spec.MaxTaskDuration != models.Interval(time.Duration(0)) {
		var cancel context.CancelFunc
		ctx, cancel = utils.CombinedContext(r.chStop, time.Duration(spec.MaxTaskDuration))
		defer cancel()
	} else {
		ctx = context.Background()
	}

	result := task.Run(ctx, taskRun, inputs)
	if _, is := result.Error.(FinalErrors); !is && result.Error != nil {
		logger.Errorw("Pipeline task run errored", append(loggerFields, "error", result.Error)...)
	} else {
		f := append(loggerFields, "result", result.Value)
		switch v := result.Value.(type) {
		case []byte:
			f = append(f, "resultString", fmt.Sprintf("%q", v))
			f = append(f, "resultHex", fmt.Sprintf("%x", v))
		}
		logger.Debugw("Pipeline task completed", f...)
	}

	return result
}

func (r *runner) runReaper() {
	err := r.orm.DeleteRunsOlderThan(r.config.JobPipelineReaperThreshold())
	if err != nil {
		logger.Errorw("Pipeline run reaper failed", "error", err)
	}
}
