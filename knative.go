package corral

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/bcongdon/corral/internal/pkg/corfs"
)

var (
	knativeDriver *Driver
)

// runningInKnative infers if the program is running in Knative via inspection of the environment
// TODO: check for any envvars that are present by default
func runningInKnative() bool {
	// ALL of the following envvars are expected
	expectedEnvVars := []string{"KNATIVE"}
	for _, envVar := range expectedEnvVars {
		if os.Getenv(envVar) == "" {
			return false
		}
	}
	return true
}

type knativeExecutor struct {
	serviceName string
}

func newKnativeExecutor(serviceName string) *knativeExecutor {
	return &knativeExecutor{
		serviceName: serviceName,
	}
}

func (k *knativeExecutor) RunMapper(job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
	mapTask := task{
		JobNumber:        jobNumber,
		Phase:            MapPhase,
		BinID:            binID,
		Splits:           inputSplits,
		IntermediateBins: job.intermediateBins,
		FileSystemType:   corfs.S3,
		WorkingLocation:  job.outputPath,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := k.invoke(payload)
	taskResult := knativeLoadTaskResult(resultPayload)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (k *knativeExecutor) RunReducer(job *Job, jobNumber int, binID uint) error {
	mapTask := task{
		JobNumber:       jobNumber,
		Phase:           ReducePhase,
		BinID:           binID,
		FileSystemType:  corfs.S3,
		WorkingLocation: job.outputPath,
		Cleanup:         job.config.Cleanup,
	}
	payload, err := json.Marshal(mapTask)
	if err != nil {
		return err
	}

	resultPayload, err := k.invoke(payload)
	taskResult := knativeLoadTaskResult(resultPayload)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (k *knativeExecutor) Deploy() {
	log.Fatal("NOT YET IMPLEMENTED")
}

func (k *knativeExecutor) Undeploy() {
	log.Fatal("NOT YET IMPLEMENTED")
}

func (k *knativeExecutor) Start() {
	s := &http.Server{
		Addr:           ":80",
		Handler:        k,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	if err := s.ListenAndServe(); err != nil {
		log.Fatal("HTTP server failed: ", err)
	}
}

func (k *knativeExecutor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error("Failed to read http body: ", err)
	}
	var task task
	if err := json.Unmarshal(body, &task); err != nil {
		log.Error("Failed to unmarshal: ", err)
	}
	s, err := knativeHandleRequest(task)
	if err != nil {
		log.Error("Failed to handle request: ", err)
	}
	if _, err := w.Write([]byte(s)); err != nil {
		log.Error("Failed to write task result: ", err)
	}
}

func knativeHandleRequest(task task) (string, error) {
	// Setup current job
	fs := corfs.InitFilesystem(task.FileSystemType)
	currentJob := knativeDriver.jobs[task.JobNumber]
	currentJob.fileSystem = fs
	currentJob.intermediateBins = task.IntermediateBins
	currentJob.outputPath = task.WorkingLocation
	currentJob.config.Cleanup = task.Cleanup

	if task.Phase == MapPhase {
		err := currentJob.runMapper(task.BinID, task.Splits)
		return prepareResult(currentJob), err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(task.BinID)
		return prepareResult(currentJob), err
	}
	return "", fmt.Errorf("unknown phase: %d", task.Phase)
}

func knativeLoadTaskResult(payload []byte) taskResult {
	var result taskResult
	err := json.Unmarshal(payload, &result)
	if err != nil {
		log.Errorf("%s", err)
	}
	return result
}

func (k *knativeExecutor) invoke(payload []byte) (outputPayload []byte, err error) {
	url := fmt.Sprintf("http://%s.default.127.0.0.1.nip.io:31080/", k.serviceName)
	res, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}
