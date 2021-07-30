package corral

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	tracing "github.com/ease-lab/vhive/utils/tracing/go"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/bcongdon/corral/internal/pkg/corfs"
)

type corralServer struct {
	UnimplementedCorralServer
}

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
	serviceURL string
}

func newKnativeExecutor(serviceURL string) *knativeExecutor {
	return &knativeExecutor{
		serviceURL: serviceURL,
	}
}

func (k *knativeExecutor) RunMapper(ctx context.Context, job *Job, jobNumber int, binID uint, inputSplits []inputSplit) error {
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

	resultPayload, err := k.invoke(ctx, payload)
	taskResult := knativeLoadTaskResult(resultPayload)

	atomic.AddInt64(&job.bytesRead, int64(taskResult.BytesRead))
	atomic.AddInt64(&job.bytesWritten, int64(taskResult.BytesWritten))

	return err
}

func (k *knativeExecutor) RunReducer(ctx context.Context, job *Job, jobNumber int, binID uint) error {
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

	resultPayload, err := k.invoke(ctx, payload)
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
	port := os.Getenv("PORT")
	if port == "" {
		log.Warn("PORT envvar is missing, defaulting to 80")
		port = "80"
	}

	var grpcServer *grpc.Server
	if tracing.IsTracingEnabled() {
		grpcServer = tracing.GetGRPCServerWithUnaryInterceptor()
	} else {
		grpcServer = grpc.NewServer()
	}

	var corralServer corralServer

	RegisterCorralServer(grpcServer, &corralServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal("Failed to listen: ", err)
	}

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("Failed to serve: ", err)
	}
}

func (cs *corralServer) Invoke(ctx context.Context, req *CorralRequest) (*CorralResponse, error) {
	var task task
	if err := json.Unmarshal(req.Payload, &task); err != nil {
		log.Error("Failed to unmarshal: ", err)
		return nil, err
	}
	s, err := knativeHandleRequest(ctx, task)
	if err != nil {
		log.Error("Failed to handle request: ", err)
		return nil, err
	}
	return &CorralResponse{Payload: []byte(s)}, nil
}

func knativeHandleRequest(ctx context.Context, task task) (string, error) {
	// Setup current job
	fs := corfs.InitFilesystem(task.FileSystemType)
	currentJob := knativeDriver.jobs[task.JobNumber]
	currentJob.fileSystem = fs
	currentJob.intermediateBins = task.IntermediateBins
	currentJob.outputPath = task.WorkingLocation
	currentJob.config.Cleanup = task.Cleanup

	if task.Phase == MapPhase {
		err := currentJob.runMapper(ctx, task.BinID, task.Splits)
		return prepareResult(currentJob), err
	} else if task.Phase == ReducePhase {
		err := currentJob.runReducer(ctx, task.BinID)
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

func (k *knativeExecutor) invoke(ctx context.Context, payload []byte) (outputPayload []byte, err error) {
	dialOptions := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}
	if tracing.IsTracingEnabled() {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	}
	conn, err := grpc.Dial(k.serviceURL, dialOptions...)
	if err != nil {
		log.Fatal("Failed to dial: ", err)
	}
	defer conn.Close()

	client := NewCorralClient(conn)
	resp, err := client.Invoke(ctx, &CorralRequest{
		Payload: payload,
	})
	if err != nil {
		log.Fatal("Failed to invoke: ", err)
	}

	return resp.Payload, nil
}
