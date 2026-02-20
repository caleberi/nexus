package nexus

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writeTestPluginSource(t *testing.T, dir string, name string) {
	t.Helper()

	source := "package main\n\n" +
		"import \"grain/nexus\"\n\n" +
		"type testPlugin struct{}\n\n" +
		"func (p testPlugin) Meta() nexus.PluginMeta {\n" +
		"\treturn nexus.PluginMeta{Name: \"" + name + "\", Version: 1}\n" +
		"}\n\n" +
		"func Plugin(_ string) (nexus.Plugin, error) {\n" +
		"\treturn testPlugin{}, nil\n" +
		"}\n"

	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(source), 0644); err != nil {
		t.Fatalf("write plugin source: %v", err)
	}
}

func TestNewPluginBuilder_Errors(t *testing.T) {
	t.Parallel()

	missingDir := filepath.Join(os.TempDir(), "missing-dir-"+t.Name())
	if _, err := NewPluginBuilder(missingDir, "*.go"); err == nil {
		t.Fatalf("expected error for missing dir")
	}

	existingDir, err := os.MkdirTemp(".", "plugintest-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(existingDir)

	if _, err := NewPluginBuilder(existingDir, "[["); err == nil {
		t.Fatalf("expected error for invalid glob")
	}
}

func TestPluginBuilder_LocateAndBuildPlugins(t *testing.T) {
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

	builder, err := NewPluginBuilder(baseDir, "*.go")
	if err != nil {
		t.Fatalf("new builder: %v", err)
	}

	if err := builder.LocateAndBuildPlugins(outputDir); err != nil {
		t.Fatalf("build plugins: %v", err)
	}

	outputFile := filepath.Join(outputDir, "alpha.so")
	if _, err := os.Stat(outputFile); err != nil {
		t.Fatalf("expected plugin output %s: %v", outputFile, err)
	}
}
