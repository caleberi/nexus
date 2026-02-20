package nexus

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func buildTestPlugin(t *testing.T, pluginDir string, outputDir string) string {
	t.Helper()

	builder, err := NewPluginBuilder(pluginDir, "*.go")
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}
	if err := builder.LocateAndBuildPlugins(outputDir); err != nil {
		t.Fatalf("build plugins: %v", err)
	}

	return filepath.Join(outputDir, filepath.Base(pluginDir)+".so")
}

func TestPluginLoader_LoadPlugins(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("go plugins are not supported on Windows")
	}

	baseDir, err := os.MkdirTemp(".", "plugintest-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	pluginDir := filepath.Join(baseDir, "alpha")
	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		t.Fatalf("create plugin dir: %v", err)
	}

	writeTestPluginSource(t, pluginDir, "test.alpha")

	outputDir, err := os.MkdirTemp(".", "pluginout-")
	if err != nil {
		t.Fatalf("create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	buildTestPlugin(t, pluginDir, outputDir)

	if err := os.WriteFile(filepath.Join(outputDir, "junk.txt"), []byte("ignore"), 0644); err != nil {
		t.Fatalf("write junk file: %v", err)
	}

	loader, err := NewPluginLoader(outputDir)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	if err := loader.LoadPlugins(); err != nil {
		if strings.Contains(err.Error(), "different version of package internal/runtime/sys") {
			t.Skipf("plugin runtime mismatch: %v", err)
		}
		t.Fatalf("load plugins: %v", err)
	}

	plugins := loader.GetPlugins()
	if len(plugins) != 1 {
		t.Fatalf("expected 1 plugin, got %d", len(plugins))
	}
	if _, ok := plugins["test.alpha"]; !ok {
		t.Fatalf("expected plugin test.alpha to be loaded")
	}
}
