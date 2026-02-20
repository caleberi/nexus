package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"grain/nexus"
)

// ExamplePlugin is a simple template for creating Nexus plugins via the web UI
type ExamplePlugin struct{}

// ExampleArgs defines the arguments accepted by this plugin
type ExampleArgs struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

// Meta returns plugin metadata
func (p *ExamplePlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "example.WebPlugin",
		Description: "Example plugin created via web UI",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "count": {"type": "integer", "default": 1}
            },
            "required": ["message"]
        }`),
	}
}

// Execute runs the plugin logic
func (p *ExamplePlugin) Execute(ctx context.Context, args ExampleArgs) (string, error) {
	if args.Message == "" {
		return "", fmt.Errorf("message is required")
	}
	if args.Count <= 0 {
		args.Count = 1
	}

	var output strings.Builder
	for index := 0; index < args.Count; index++ {
		fmt.Fprintf(&output, "[%d] %s\n", index+1, args.Message)
	}
	return output.String(), nil
}

// Plugin is the loader function that the Nexus plugin system calls to instantiate the plugin
func Plugin(_ string) (nexus.Plugin, error) {
	return &ExamplePlugin{}, nil
}
