package corfs

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// LocalFileSystem wraps "os" to provide access to the local filesystem.
type LocalFileSystem struct{}

func walkDir(dir string) []FileInfo {
	files := make([]FileInfo, 0)
	if err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Error(err)
			return err
		}
		if f.IsDir() {
			return nil
		}
		files = append(files, FileInfo{
			Name: path,
			Size: f.Size(),
		})
		return nil
	}); err != nil {
		log.Fatal("Failed to walk directory: ", err)
	}

	return files
}

// ListFiles lists files that match pathGlob.
func (l *LocalFileSystem) ListFiles(pathGlob string) ([]FileInfo, error) {
	globbedFiles, err := filepath.Glob(pathGlob)
	if err != nil {
		return nil, err
	}

	files := make([]FileInfo, 0)
	for _, fileName := range globbedFiles {
		fInfo, err := os.Stat(fileName)
		if err != nil {
			log.Error(err)
			continue
		}
		if !fInfo.IsDir() {
			files = append(files, FileInfo{
				Name: fileName,
				Size: fInfo.Size(),
			})
		} else {
			files = append(files, walkDir(fileName)...)
		}
	}

	return files, err
}

// OpenReader opens a reader to the file at filePath. The reader
// is initially seeked to "startAt" bytes into the file.
// func (l *LocalFileSystem) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
// 	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
// 	if err != nil {
// 		return nil, err
// 	}
// 	_, err = file.Seek(startAt, io.SeekStart)
// 	return file, err
// }

// ReadFile reads the file at filePath skipping startAt bytes at the
// beginning.
func (l *LocalFileSystem) ReadFile(filePath string, startAt int64) ([]byte, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	if _, err = file.Seek(startAt, io.SeekStart); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}

// OpenWriter opens a writer to the file at filePath.
func (l *LocalFileSystem) OpenWriter(filePath string) (io.WriteCloser, error) {
	dir := filepath.Dir(filePath)

	// Create writer directory if necessary
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, err
		}
	}

	return os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
}

// Stat returns information about the file at filePath.
// func (l *LocalFileSystem) Stat(filePath string) (FileInfo, error) {
// 	fInfo, err := os.Stat(filePath)
// 	if err != nil {
// 		return FileInfo{}, err
// 	}
// 	return FileInfo{
// 		Name: filePath,
// 		Size: fInfo.Size(),
// 	}, nil
// }

// Init initializes the filesystem.
func (l *LocalFileSystem) Init() error {
	return nil
}

// Join joins file path elements
func (l *LocalFileSystem) Join(elem ...string) string {
	return filepath.Join(elem...)
}

// Delete deletes the file at filePath.
// func (l *LocalFileSystem) Delete(filePath string) error {
// 	return os.Remove(filePath)
// }
