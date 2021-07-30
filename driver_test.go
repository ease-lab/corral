package corral

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDriver(t *testing.T) {
	j := &Job{}
	driver := NewDriver(
		j,
		WithSplitSize(100),
		WithMapBinSize(200),
		WithReduceBinSize(300),
		WithWorkingLocation("s3://foo"),
	)

	assert.Equal(t, j, driver.jobs[0])
	assert.Equal(t, int64(100), driver.config.SplitSize)
	assert.Equal(t, int64(200), driver.config.MapBinSize)
	assert.Equal(t, int64(300), driver.config.ReduceBinSize)
	assert.Equal(t, "s3://foo", driver.config.WorkingLocation)
}

type testWCJob struct{}

func (testWCJob) Map(ctx context.Context, key, value string, emitter Emitter) {
	for _, word := range strings.Fields(value) {
		if err := emitter.Emit(ctx, word, "1"); err != nil {
			panic(err)
		}
	}
}

func (testWCJob) Reduce(ctx context.Context, key string, values ValueIterator, emitter Emitter) {
	count := 0
	for range values.Iter() {
		count++
	}
	if err := emitter.Emit(ctx, key, fmt.Sprintf("%d", count)); err != nil {
		panic(err)
	}
}

type testFilterJob struct {
	prefix string
}

func (j *testFilterJob) Map(ctx context.Context, key, value string, emitter Emitter) {
	if strings.HasPrefix(key, j.prefix) {
		if err := emitter.Emit(ctx, key, value); err != nil {
			panic(err)
		}
	}
}

func (j *testFilterJob) Reduce(ctx context.Context, key string, values ValueIterator, emitter Emitter) {
	// Identity reducer
	for value := range values.Iter() {
		if err := emitter.Emit(ctx, key, value); err != nil {
			panic(err)
		}
	}
}

func testOutputToKeyValues(output string) []keyValue {
	lines := strings.Split(output, "\n")
	keyVals := make([]keyValue, 0, len(lines))

	for _, line := range lines {
		split := strings.Split(line, "\t")
		if len(split) != 2 {
			continue
		}
		keyVals = append(keyVals, keyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	return keyVals
}

func TestLocalMapReduce(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	inputPath := filepath.Join(tmpdir, "test_input")
	assert.NoError(t, ioutil.WriteFile(inputPath, []byte("the test input\nthe input test\nfoo bar baz"), 0700))

	job := NewJob(testWCJob{}, testWCJob{})
	driver := NewDriver(
		job,
		WithInputs(tmpdir),
		WithWorkingLocation(tmpdir),
	)

	driver.Main(context.Background())

	output, err := ioutil.ReadFile(filepath.Join(tmpdir, "output-part-0"))
	assert.Nil(t, err)

	keyVals := testOutputToKeyValues(string(output))
	assert.Len(t, keyVals, 6)

	correct := []keyValue{
		{"the", "2"},
		{"test", "2"},
		{"input", "2"},
		{"foo", "1"},
		{"bar", "1"},
		{"baz", "1"},
	}
	for _, kv := range correct {
		assert.Contains(t, keyVals, kv)
	}
}

func TestLocalMultiJob(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	inputPath := filepath.Join(tmpdir, "test_input")
	assert.NoError(t, ioutil.WriteFile(inputPath, []byte("the test input\nthe input test\nfoo bar baz"), 0700))

	mr1 := testWCJob{}
	job1 := NewJob(mr1, mr1)

	// Second job filters out any keys that don't start with 't'
	mr2 := &testFilterJob{prefix: "t"}
	job2 := NewJob(mr2, mr2)

	driver := NewMultiStageDriver([]*Job{job1, job2},
		WithInputs(tmpdir),
		WithWorkingLocation(tmpdir),
	)

	driver.Main(context.Background())

	output, err := ioutil.ReadFile(filepath.Join(tmpdir, "job1", "output-part-0"))
	assert.Nil(t, err)

	keyVals := testOutputToKeyValues(string(output))
	assert.Len(t, keyVals, 2)

	correct := []keyValue{
		{"the", "2"},
		{"test", "2"},
	}
	for _, kv := range correct {
		assert.Contains(t, keyVals, kv)
	}
}

func TestLocalNoCrashOnNoResolvedInputFiles(t *testing.T) {
	job := NewJob(testWCJob{}, testWCJob{})
	driver := NewDriver(
		job,
		WithInputs("does_not_exist"),
		WithWorkingLocation("some_file"),
	)

	driver.Main(context.Background())
}

type statefulJob struct {
	filterWords *[]string
}

func (s statefulJob) Map(ctx context.Context, key, value string, emitter Emitter) {
	for _, word := range strings.Fields(value) {
		for _, filterWord := range *s.filterWords {
			if filterWord != word {
				continue
			}
			if err := emitter.Emit(ctx, word, "1"); err != nil {
				panic(err)
			}
		}
	}
}

func (statefulJob) Reduce(ctx context.Context, key string, values ValueIterator, emitter Emitter) {
	count := 0
	for range values.Iter() {
		count++
	}
	if err := emitter.Emit(ctx, key, fmt.Sprintf("%d", count)); err != nil {
		panic(err)
	}
}

func TestLocalStructFieldMapReduce(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)

	inputPath := filepath.Join(tmpdir, "test_input")
	assert.NoError(t, ioutil.WriteFile(inputPath, []byte("the test input\nthe input test\nfoo bar baz"), 0700))

	jobStruct := statefulJob{filterWords: &[]string{"foo", "bar"}}
	job := NewJob(jobStruct, jobStruct)
	driver := NewDriver(
		job,
		WithInputs(tmpdir),
		WithWorkingLocation(tmpdir),
	)

	driver.Main(context.Background())

	output, err := ioutil.ReadFile(filepath.Join(tmpdir, "output-part-0"))
	assert.Nil(t, err)

	keyVals := testOutputToKeyValues(string(output))
	assert.Len(t, keyVals, 2)

	correct := []keyValue{
		{"foo", "1"},
		{"bar", "1"},
	}
	for _, kv := range correct {
		assert.Contains(t, keyVals, kv)
	}
}
