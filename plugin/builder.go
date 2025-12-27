package plugin

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gobwas/glob"
)

type PluginBuilder struct {
	dirPath   string
	glob      glob.Glob
	errChan   chan error
	filePaths chan string
}

func NewPluginBuilder(dirPath string, fileGlob string) (*PluginBuilder, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current directory: %s", cwd)
	}

	stat, err := os.Lstat(filepath.Clean(filepath.Join(cwd, dirPath)))
	if err != nil {
		return nil, fmt.Errorf("failed to stat directory %s: %v", dirPath, err)
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf("the specified path %s is not a directory", dirPath)
	}

	gb, err := glob.Compile(fileGlob)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %s: %v", fileGlob, err)
	}

	return &PluginBuilder{
		dirPath:   dirPath,
		glob:      gb,
		errChan:   make(chan error, 1),
		filePaths: make(chan string, 1),
	}, nil
}

func (pb *PluginBuilder) walker(path string, d fs.DirEntry, err error) error {
	if err != nil {
		pb.errChan <- err
		return err
	}

	if d.IsDir() {
		return nil
	}

	if d.Type().IsRegular() && pb.glob.Match(d.Name()) {
		pb.filePaths <- path
	}
	return nil
}

func (pb *PluginBuilder) LocateAndBuildPlugins(buildPath string) error {
	outputDir := buildPath
	if outputDir == "" {
		outputDir = "../../build"
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %s", cwd)
	}

	outputDir = filepath.Clean(filepath.Join(cwd, buildPath))
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %v", outputDir, err)
	}

	go func() {
		defer close(pb.filePaths)
		defer close(pb.errChan)
		if err := fs.WalkDir(os.DirFS(pb.dirPath), ".", pb.walker); err != nil {
			pb.errChan <- err
		}
	}()

	pluginDirs := make(map[string]bool)
	for path := range pb.filePaths {
		pluginDir := filepath.Dir(path)
		pluginDirs[pluginDir] = true
	}

	select {
	case err := <-pb.errChan:
		if err != nil {
			return fmt.Errorf("error during directory walk: %v", err)
		}
	default:
	}

	var buildErrors []error
	for pluginDir := range pluginDirs {
		absPluginDir := filepath.Join(pb.dirPath, pluginDir)
		dirName := filepath.Base(pluginDir)
		if dirName == "." {
			dirName = filepath.Base(pb.dirPath)
		}
		outputFile := filepath.Join(outputDir, dirName+".so")
		cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", outputFile, ".")
		cmd.Dir = absPluginDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			buildErrors = append(buildErrors, fmt.Errorf("failed to build plugin in %s: %v", absPluginDir, err))
		}
	}

	if len(buildErrors) > 0 {
		return fmt.Errorf("encountered %d build errors: %v", len(buildErrors), buildErrors)
	}

	return nil
}
