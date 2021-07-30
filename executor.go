package corral

import "context"

type executor interface {
	RunMapper(ctx context.Context, job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error
	RunReducer(ctx context.Context, job *Job, jobNumber int, binID uint) error
}

type localExecutor struct{}

func (localExecutor) RunMapper(ctx context.Context, job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	return job.runMapper(ctx, binID, inputSplits)
}

func (localExecutor) RunReducer(ctx context.Context, job *Job, jobNumber int, binID uint) error {
	return job.runReducer(ctx, binID)
}
