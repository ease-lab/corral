package corral

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/bcongdon/corral/internal/pkg/corfs"
)

// Job is the logical container for a MapReduce job
type Job struct {
	Map           Mapper
	Reduce        Reducer
	PartitionFunc PartitionFunc

	fileSystem       corfs.FileSystem
	config           *config
	intermediateBins uint
	outputPath       string

	bytesRead    int64
	bytesWritten int64
}

// Logic for running a single map task
func (j *Job) runMapper(ctx context.Context, mapperID uint, splits []inputSplit) error {
	emitter := newMapperEmitter(j.intermediateBins, mapperID, j.outputPath, j.fileSystem)
	if j.PartitionFunc != nil {
		emitter.partitionFunc = j.PartitionFunc
	}

	for _, split := range splits {
		err := j.runMapperSplit(ctx, split, &emitter)
		if err != nil {
			return err
		}
	}

	atomic.AddInt64(&j.bytesWritten, emitter.bytesWritten())

	return emitter.close()
}

func splitInputRecord(record string) *keyValue {
	fields := strings.Split(record, "\t")
	if len(fields) == 2 {
		return &keyValue{
			Key:   fields[0],
			Value: fields[1],
		}
	}
	return &keyValue{
		Value: record,
	}
}

// runMapperSplit runs the mapper on a single inputSplit
func (j *Job) runMapperSplit(ctx context.Context, split inputSplit, emitter Emitter) error {
	// inputSource, err := j.fileSystem.OpenReader(split.Filename, split.StartOffset)
	// if err != nil {
	// 	return err
	// }
	input, err := j.fileSystem.ReadFile(split.Filename, split.StartOffset)
	if err != nil {
		return err
	}

	// scanner := bufio.NewScanner(inputSource)
	scanner := bufio.NewScanner(bytes.NewReader(input))
	var bytesRead int64
	splitter := countingSplitFunc(bufio.ScanLines, &bytesRead)
	scanner.Split(splitter)

	if split.StartOffset != 0 {
		scanner.Scan()
	}

	for scanner.Scan() {
		record := scanner.Text()
		kv := splitInputRecord(record)
		j.Map.Map(ctx, kv.Key, kv.Value, emitter)

		// Stop reading when end of inputSplit is reached
		pos := bytesRead
		if split.Size() > 0 && pos > split.Size() {
			break
		}
	}

	atomic.AddInt64(&j.bytesRead, bytesRead)

	return nil
}

// Logic for running a single reduce task
func (j *Job) runReducer(ctx context.Context, binID uint) error {
	// Determine the intermediate data files this reducer is responsible for
	path := j.fileSystem.Join(j.outputPath, fmt.Sprintf("map-bin%d-*", binID))
	files, err := j.fileSystem.ListFiles(path)
	if err != nil {
		return err
	}

	// Open emitter for output data
	path = j.fileSystem.Join(j.outputPath, fmt.Sprintf("output-part-%d", binID))
	// emitWriter, err := j.fileSystem.OpenWriter(path)
	// if err != nil {
	// 	return err
	// }
	// defer emitWriter.Close()
	buffer := new(bytes.Buffer)

	data := make(map[string][]string)
	var bytesRead int64

	for _, file := range files {
		// reader, err := j.fileSystem.OpenReader(file.Name, 0)
		// bytesRead += file.Size
		// if err != nil {
		// 	return err
		// }
		fileContent, err := j.fileSystem.ReadFile(file.Name, 0)
		bytesRead += int64(len(fileContent))
		if err != nil {
			return err
		}
		reader := bytes.NewReader(fileContent)

		// Feed intermediate data into reducers
		decoder := json.NewDecoder(reader)
		for decoder.More() {
			var kv keyValue
			if err := decoder.Decode(&kv); err != nil {
				return err
			}

			if _, ok := data[kv.Key]; !ok {
				data[kv.Key] = make([]string, 0)
			}

			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
		// reader.Close()

		// Delete intermediate map data
		// if j.config.Cleanup {
		// 	err := j.fileSystem.Delete(file.Name)
		// 	if err != nil {
		// 		log.Error(err)
		// 	}
		// }
	}

	var waitGroup sync.WaitGroup
	sem := semaphore.NewWeighted(10)

	emitter := newReducerEmitter(buffer)
	for key, values := range data {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("failed to run reducer: failed to acquire semaphore: %s", err)
		}
		waitGroup.Add(1)
		go func(key string, values []string) {
			defer sem.Release(1)

			keyChan := make(chan string)
			keyIter := newValueIterator(keyChan)

			go func() {
				defer waitGroup.Done()
				j.Reduce.Reduce(ctx, key, keyIter, emitter)
			}()

			for _, value := range values {
				// Pass current value to the appropriate key channel
				keyChan <- value
			}
			close(keyChan)
		}(key, values)
	}

	waitGroup.Wait()

	atomic.AddInt64(&j.bytesWritten, emitter.bytesWritten())
	atomic.AddInt64(&j.bytesRead, bytesRead)

	return j.fileSystem.WriteFile(path, buffer.Bytes())
}

// inputSplits calculates all input files' inputSplits.
// inputSplits also determines and saves the number of intermediate bins that will be used during the shuffle.
func (j *Job) inputSplits(inputs []string, maxSplitSize int64) []inputSplit {
	fileInfos := make([]corfs.FileInfo, 0)
	for _, inputPath := range inputs {
		fileList, err := j.fileSystem.ListFiles(inputPath)
		if err != nil {
			log.Warn(err)
			continue
		}

		for _, fInfo := range fileList {
			fileInfos = append(fileInfos, fInfo)
		}
	}

	splits := make([]inputSplit, 0)
	var totalSize int64
	for _, fInfo := range fileInfos {
		totalSize += fInfo.Size
		splits = append(splits, splitInputFile(fInfo, maxSplitSize)...)
	}
	if len(fileInfos) > 0 {
		log.Debugf("Average split size: %s bytes", humanize.Bytes(uint64(totalSize)/uint64(len(splits))))
	}

	j.intermediateBins = uint(float64(totalSize/j.config.ReduceBinSize) * 1.25)
	if j.intermediateBins == 0 {
		j.intermediateBins = 1
	}

	return splits
}

// NewJob creates a new job from a Mapper and Reducer.
func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:    mapper,
		Reduce: reducer,
		config: &config{},
	}
}
