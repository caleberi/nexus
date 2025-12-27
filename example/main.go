package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"grain/nexus"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/olekukonko/tablewriter"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DummyPlugin struct{}

type DummyArgs struct {
	Message string `json:"message"`
}

func (d *DummyPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:           "dummy.Dummy",
		Description:    "A dummy plugin for stress testing",
		Version:        1,
		ArgsSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string"}}}`),
		FormSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string", "title": "Message"}}}`),
	}
}

func (d *DummyPlugin) Execute(ctx context.Context, args DummyArgs) (string, error) {
	f, err := os.OpenFile("plugin_output.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	msg := fmt.Sprintf("Processed message: %s at %s\n", args.Message, time.Now().Format(time.RFC3339Nano))
	_, err = fmt.Fprint(f, msg)
	if err != nil {
		return "", err
	}

	time.Sleep(5 * time.Millisecond)
	return msg, nil
}

type ImageGeneratorPlugin struct{}

type ImageGeneratorArgs struct {
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Pattern     string `json:"pattern"`     // "gradient", "circles", "squares", "stripes"
	ColorScheme string `json:"colorScheme"` // "blue", "red", "green", "rainbow", "random"
	Filename    string `json:"filename"`
}

func (i *ImageGeneratorPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "image.Generator",
		Description: "Generates simple geometric pattern images",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"width": {"type": "integer", "minimum": 100, "maximum": 2000, "default": 800},
				"height": {"type": "integer", "minimum": 100, "maximum": 2000, "default": 600},
				"pattern": {"type": "string", "enum": ["gradient", "circles", "squares", "stripes"], "default": "gradient"},
				"colorScheme": {"type": "string", "enum": ["blue", "red", "green", "rainbow", "random"], "default": "blue"},
				"filename": {"type": "string", "default": "generated_image.png"}
			},
			"required": ["width", "height", "pattern", "colorScheme", "filename"]
		}`),
		FormSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"width": {"type": "integer", "title": "Width", "minimum": 100, "maximum": 2000, "default": 800},
				"height": {"type": "integer", "title": "Height", "minimum": 100, "maximum": 2000, "default": 600},
				"pattern": {"type": "string", "title": "Pattern", "enum": ["gradient", "circles", "squares", "stripes"], "default": "gradient"},
				"colorScheme": {"type": "string", "title": "Color Scheme", "enum": ["blue", "red", "green", "rainbow", "random"], "default": "blue"},
				"filename": {"type": "string", "title": "Filename", "default": "generated_image.png"}
			}
		}`),
	}
}

func (i *ImageGeneratorPlugin) Execute(ctx context.Context, args ImageGeneratorArgs) (string, error) {
	outputDir := "generated_images"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	img := image.NewRGBA(image.Rect(0, 0, args.Width, args.Height))

	switch args.Pattern {
	case "gradient":
		i.generateGradient(img, args)
	case "circles":
		i.generateCircles(img, args)
	case "squares":
		i.generateSquares(img, args)
	case "stripes":
		i.generateStripes(img, args)
	default:
		i.generateGradient(img, args)
	}

	filename := filepath.Join(outputDir, args.Filename)
	file, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("failed to create image file: %w", err)
	}
	defer file.Close()

	if err := png.Encode(file, img); err != nil {
		return "", fmt.Errorf("failed to encode PNG: %w", err)
	}

	result := fmt.Sprintf("Generated image: %s (%dx%d, pattern: %s, colors: %s) at %s",
		filename, args.Width, args.Height, args.Pattern, args.ColorScheme, time.Now().Format(time.RFC3339))

	return result, nil
}

func (i *ImageGeneratorPlugin) getColorPalette(scheme string) []color.RGBA {
	switch scheme {
	case "blue":
		return []color.RGBA{
			{0, 100, 200, 255},
			{50, 150, 255, 255},
			{100, 200, 255, 255},
			{150, 220, 255, 255},
		}
	case "red":
		return []color.RGBA{
			{200, 50, 50, 255},
			{255, 100, 100, 255},
			{255, 150, 150, 255},
			{255, 200, 200, 255},
		}
	case "green":
		return []color.RGBA{
			{50, 150, 50, 255},
			{100, 200, 100, 255},
			{150, 255, 150, 255},
			{200, 255, 200, 255},
		}
	case "rainbow":
		return []color.RGBA{
			{255, 0, 0, 255},   // Red
			{255, 165, 0, 255}, // Orange
			{255, 255, 0, 255}, // Yellow
			{0, 255, 0, 255},   // Green
			{0, 0, 255, 255},   // Blue
			{75, 0, 130, 255},  // Indigo
			{148, 0, 211, 255}, // Violet
		}
	case "random":
		source := rand.NewSource(time.Now().Unix())
		r := rand.New(source)
		palette := make([]color.RGBA, 5)
		for i := range palette {
			palette[i] = color.RGBA{
				uint8(r.Intn(256)),
				uint8(r.Intn(256)),
				uint8(r.Intn(256)),
				255,
			}
		}
		return palette
	default:
		return i.getColorPalette("blue")
	}
}

func (i *ImageGeneratorPlugin) generateGradient(img *image.RGBA, args ImageGeneratorArgs) {
	palette := i.getColorPalette(args.ColorScheme)
	bounds := img.Bounds()

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			ratio := float64(y) / float64(bounds.Max.Y)
			colorIndex := int(ratio * float64(len(palette)-1))
			if colorIndex >= len(palette)-1 {
				colorIndex = len(palette) - 1
			}

			nextIndex := colorIndex + 1
			if nextIndex >= len(palette) {
				nextIndex = colorIndex
			}

			localRatio := (ratio * float64(len(palette)-1)) - float64(colorIndex)

			c1 := palette[colorIndex]
			c2 := palette[nextIndex]

			r := uint8(float64(c1.R)*(1-localRatio) + float64(c2.R)*localRatio)
			g := uint8(float64(c1.G)*(1-localRatio) + float64(c2.G)*localRatio)
			b := uint8(float64(c1.B)*(1-localRatio) + float64(c2.B)*localRatio)

			img.Set(x, y, color.RGBA{r, g, b, 255})
		}
	}
}

func (i *ImageGeneratorPlugin) generateCircles(img *image.RGBA, args ImageGeneratorArgs) {
	palette := i.getColorPalette(args.ColorScheme)
	bounds := img.Bounds()

	draw.Draw(img, bounds, &image.Uniform{palette[0]}, image.ZP, draw.Src)

	source := rand.NewSource(time.Now().Unix())
	r := rand.New(source)
	numCircles := r.Intn(100)

	for range numCircles {
		centerX := r.Intn(args.Width)
		centerY := r.Intn(args.Height)
		radius := r.Intn(min(args.Width, args.Height)/4) + 20
		colorIndex := r.Intn(len(palette))

		i.drawCircle(img, centerX, centerY, radius, palette[colorIndex])
	}
}

func (i *ImageGeneratorPlugin) generateSquares(img *image.RGBA, args ImageGeneratorArgs) {
	palette := i.getColorPalette(args.ColorScheme)
	bounds := img.Bounds()

	draw.Draw(img, bounds, &image.Uniform{palette[0]}, image.ZP, draw.Src)

	source := rand.NewSource(time.Now().Unix())
	r := rand.New(source)
	squareSize := 50

	for y := 0; y < args.Height; y += squareSize {
		for x := 0; x < args.Width; x += squareSize {
			if rand.Float32() > 0.3 { // 70% chance to draw a square
				colorIndex := r.Intn(len(palette))
				rect := image.Rect(x, y, x+squareSize, y+squareSize)
				draw.Draw(img, rect, &image.Uniform{palette[colorIndex]}, image.ZP, draw.Src)
			}
		}
	}
}

func (i *ImageGeneratorPlugin) generateStripes(img *image.RGBA, args ImageGeneratorArgs) {
	palette := i.getColorPalette(args.ColorScheme)
	bounds := img.Bounds()
	stripeHeight := 30

	for y := bounds.Min.Y; y < bounds.Max.Y; y += stripeHeight {
		colorIndex := (y / stripeHeight) % len(palette)
		rect := image.Rect(bounds.Min.X, y, bounds.Max.X, y+stripeHeight)
		draw.Draw(img, rect, &image.Uniform{palette[colorIndex]}, image.ZP, draw.Src)
	}
}

func (i *ImageGeneratorPlugin) drawCircle(img *image.RGBA, centerX, centerY, radius int, c color.RGBA) {
	for y := centerY - radius; y <= centerY+radius; y++ {
		for x := centerX - radius; x <= centerX+radius; x++ {
			if x >= 0 && y >= 0 && x < img.Bounds().Max.X && y < img.Bounds().Max.Y {
				dx := x - centerX
				dy := y - centerY
				if dx*dx+dy*dy <= radius*radius {
					img.Set(x, y, c)
				}
			}
		}
	}
}

type PatternDrawerPlugin struct{}

type PatternDrawerArgs struct {
	Width      int      `json:"width"`
	Height     int      `json:"height"`
	Patterns   []string `json:"patterns"` // ["checkerboard", "spiral", "waves", "mandala"]
	Colors     []string `json:"colors"`   // Hex color codes
	Filename   string   `json:"filename"`
	OutputDir  string   `json:"outputDir"`
	Complexity int      `json:"complexity"` // 1-10 for pattern detail level
}

func (p *PatternDrawerPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "pattern.Drawer",
		Description: "Draws complex geometric patterns and designs",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"width": {"type": "integer", "minimum": 100, "maximum": 3000, "default": 800},
				"height": {"type": "integer", "minimum": 100, "maximum": 3000, "default": 800},
				"patterns": {"type": "array", "items": {"type": "string", "enum": ["checkerboard", "spiral", "waves", "mandala", "hexagons", "fractals"]}, "default": ["spiral"]},
				"colors": {"type": "array", "items": {"type": "string"}, "default": ["#FF5733", "#33FF57", "#3357FF"]},
				"filename": {"type": "string", "default": "pattern.png"},
				"outputDir": {"type": "string", "default": "pattern_outputs"},
				"complexity": {"type": "integer", "minimum": 1, "maximum": 10, "default": 5}
			},
			"required": ["width", "height", "patterns", "filename"]
		}`),
		FormSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"width": {"type": "integer", "title": "Width"},
				"height": {"type": "integer", "title": "Height"},
				"patterns": {"type": "array", "title": "Patterns", "items": {"type": "string"}},
				"colors": {"type": "array", "title": "Colors (Hex)", "items": {"type": "string"}},
				"filename": {"type": "string", "title": "Filename"},
				"outputDir": {"type": "string", "title": "Output Directory"},
				"complexity": {"type": "integer", "title": "Complexity (1-10)"}
			}
		}`),
	}
}

func (p *PatternDrawerPlugin) Execute(ctx context.Context, args PatternDrawerArgs) (string, error) {
	if err := os.MkdirAll(args.OutputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	img := image.NewRGBA(image.Rect(0, 0, args.Width, args.Height))

	colors := p.parseColors(args.Colors)
	if len(colors) == 0 {
		colors = []color.RGBA{{255, 87, 51, 255}, {51, 255, 87, 255}, {51, 87, 255, 255}}
	}

	draw.Draw(img, img.Bounds(), &image.Uniform{colors[0]}, image.Point{}, draw.Src)

	for _, pattern := range args.Patterns {
		switch pattern {
		case "checkerboard":
			p.drawCheckerboard(img, colors, args.Complexity)
		case "spiral":
			p.drawSpiral(img, colors, args.Complexity)
		case "waves":
			p.drawWaves(img, colors, args.Complexity)
		case "mandala":
			p.drawMandala(img, colors, args.Complexity)
		case "hexagons":
			p.drawHexagons(img, colors, args.Complexity)
		case "fractals":
			p.drawFractals(img, colors, args.Complexity)
		}
	}

	filename := filepath.Join(args.OutputDir, args.Filename)
	file, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if err := png.Encode(file, img); err != nil {
		return "", fmt.Errorf("failed to encode PNG: %w", err)
	}

	return fmt.Sprintf("Pattern drawn: %s (patterns: %v) at %s",
		filename, args.Patterns, time.Now().Format(time.RFC3339)), nil
}

func (p *PatternDrawerPlugin) parseColors(hexColors []string) []color.RGBA {
	colors := make([]color.RGBA, 0, len(hexColors))
	for _, hex := range hexColors {
		hex = strings.TrimPrefix(hex, "#")
		if len(hex) == 6 {
			var r, g, b uint8
			fmt.Sscanf(hex, "%02x%02x%02x", &r, &g, &b)
			colors = append(colors, color.RGBA{r, g, b, 255})
		}
	}
	return colors
}

func (p *PatternDrawerPlugin) drawCheckerboard(img *image.RGBA, colors []color.RGBA, complexity int) {
	squareSize := 100 / complexity
	if squareSize < 5 {
		squareSize = 5
	}
	bounds := img.Bounds()

	for y := 0; y < bounds.Max.Y; y += squareSize {
		for x := 0; x < bounds.Max.X; x += squareSize {
			colorIndex := ((x/squareSize + y/squareSize) % len(colors))
			rect := image.Rect(x, y, x+squareSize, y+squareSize)
			draw.Draw(img, rect, &image.Uniform{colors[colorIndex]}, image.Point{}, draw.Over)
		}
	}
}

func (p *PatternDrawerPlugin) drawSpiral(img *image.RGBA, colors []color.RGBA, complexity int) {
	bounds := img.Bounds()
	centerX := bounds.Max.X / 2
	centerY := bounds.Max.Y / 2
	maxRadius := float64(min(centerX, centerY))

	steps := complexity * 360
	for i := range steps {
		angle := float64(i) * 2 * math.Pi / 360
		radius := maxRadius * float64(i) / float64(steps)

		x := centerX + int(radius*math.Cos(angle))
		y := centerY + int(radius*math.Sin(angle))

		if x >= 0 && x < bounds.Max.X && y >= 0 && y < bounds.Max.Y {
			colorIndex := i % len(colors)
			p.drawCircle(img, x, y, 2+complexity/2, colors[colorIndex])
		}
	}
}

func (p *PatternDrawerPlugin) drawWaves(img *image.RGBA, colors []color.RGBA, complexity int) {
	bounds := img.Bounds()
	amplitude := float64(bounds.Max.Y) / 8
	frequency := float64(complexity) / 100

	for x := 0; x < bounds.Max.X; x++ {
		for wave := 0; wave < complexity; wave++ {
			y := bounds.Max.Y/2 + int(amplitude*math.Sin(frequency*float64(x)+float64(wave)*0.5))
			if y >= 0 && y < bounds.Max.Y {
				colorIndex := wave % len(colors)
				p.drawCircle(img, x, y, 3, colors[colorIndex])
			}
		}
	}
}

func (p *PatternDrawerPlugin) drawMandala(img *image.RGBA, colors []color.RGBA, complexity int) {
	bounds := img.Bounds()
	centerX := bounds.Max.X / 2
	centerY := bounds.Max.Y / 2
	maxRadius := float64(min(centerX, centerY)) * 0.9

	petals := 8 * complexity
	for i := 0; i < petals; i++ {
		angle := float64(i) * 2 * math.Pi / float64(petals)

		for r := 0.0; r < maxRadius; r += 5 {
			x := centerX + int(r*math.Cos(angle))
			y := centerY + int(r*math.Sin(angle))

			colorIndex := int(r/10) % len(colors)
			p.drawCircle(img, x, y, 4, colors[colorIndex])
		}
	}
}

func (p *PatternDrawerPlugin) drawHexagons(img *image.RGBA, colors []color.RGBA, complexity int) {
	bounds := img.Bounds()
	hexSize := 50 / complexity
	if hexSize < 10 {
		hexSize = 10
	}

	for y := 0; y < bounds.Max.Y; y += hexSize * 2 {
		for x := 0; x < bounds.Max.X; x += hexSize * 2 {
			colorIndex := (x + y) % len(colors)
			p.drawHexagon(img, x, y, hexSize, colors[colorIndex])
		}
	}
}

func (p *PatternDrawerPlugin) drawFractals(img *image.RGBA, colors []color.RGBA, complexity int) {
	bounds := img.Bounds()
	p.drawTriangle(img, bounds.Max.X/2, 10, bounds.Max.X/2-200, bounds.Max.Y-10,
		bounds.Max.X/2+200, bounds.Max.Y-10, colors[0], complexity)
}

func (p *PatternDrawerPlugin) drawCircle(img *image.RGBA, centerX, centerY, radius int, c color.RGBA) {
	for y := centerY - radius; y <= centerY+radius; y++ {
		for x := centerX - radius; x <= centerX+radius; x++ {
			if x >= 0 && y >= 0 && x < img.Bounds().Max.X && y < img.Bounds().Max.Y {
				dx := x - centerX
				dy := y - centerY
				if dx*dx+dy*dy <= radius*radius {
					img.Set(x, y, c)
				}
			}
		}
	}
}

func (p *PatternDrawerPlugin) drawHexagon(img *image.RGBA, cx, cy, size int, c color.RGBA) {
	for angle := 0; angle < 360; angle += 6 {
		rad := float64(angle) * math.Pi / 180
		x := cx + int(float64(size)*math.Cos(rad))
		y := cy + int(float64(size)*math.Sin(rad))
		p.drawCircle(img, x, y, 2, c)
	}
}

func (p *PatternDrawerPlugin) drawTriangle(img *image.RGBA, x1, y1, x2, y2, x3, y3 int, c color.RGBA, depth int) {
	if depth <= 0 {
		return
	}
	p.drawLine(img, x1, y1, x2, y2, c)
	p.drawLine(img, x2, y2, x3, y3, c)
	p.drawLine(img, x3, y3, x1, y1, c)
}

func (p *PatternDrawerPlugin) drawLine(img *image.RGBA, x1, y1, x2, y2 int, c color.RGBA) {
	dx := abs(x2 - x1)
	dy := abs(y2 - y1)
	steps := max(dx, dy)

	for i := 0; i <= steps; i++ {
		t := float64(i) / float64(steps)
		x := x1 + int(float64(x2-x1)*t)
		y := y1 + int(float64(y2-y1)*t)
		if x >= 0 && y >= 0 && x < img.Bounds().Max.X && y < img.Bounds().Max.Y {
			img.Set(x, y, c)
		}
	}
}

type MapReducePlugin struct{}

type MapReduceArgs struct {
	InputFile  string `json:"inputFile"`  // Path to input data file (JSON array)
	Operation  string `json:"operation"`  // "wordcount", "sum", "avg", "groupby"
	GroupByKey string `json:"groupByKey"` // For groupby operation
	OutputFile string `json:"outputFile"`
	OutputDir  string `json:"outputDir"`
	Workers    int    `json:"workers"` // Number of parallel workers
}

func (m *MapReducePlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "data.MapReduce",
		Description: "Performs map-reduce operations on data files",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"inputFile": {"type": "string"},
				"operation": {"type": "string", "enum": ["wordcount", "sum", "avg", "groupby"], "default": "wordcount"},
				"groupByKey": {"type": "string", "default": ""},
				"outputFile": {"type": "string", "default": "mapreduce_result.json"},
				"outputDir": {"type": "string", "default": "mapreduce_outputs"},
				"workers": {"type": "integer", "minimum": 1, "maximum": 16, "default": 4}
			},
			"required": ["inputFile", "operation"]
		}`),
		FormSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"inputFile": {"type": "string", "title": "Input File Path"},
				"operation": {"type": "string", "title": "Operation"},
				"groupByKey": {"type": "string", "title": "Group By Key"},
				"outputFile": {"type": "string", "title": "Output File"},
				"outputDir": {"type": "string", "title": "Output Directory"},
				"workers": {"type": "integer", "title": "Worker Count"}
			}
		}`),
	}
}

func (m *MapReducePlugin) Execute(ctx context.Context, args MapReduceArgs) (string, error) {
	if err := os.MkdirAll(args.OutputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	data, err := os.ReadFile(args.InputFile)
	if err != nil {
		return "", fmt.Errorf("failed to read input file: %w", err)
	}

	var result any

	switch args.Operation {
	case "wordcount":
		result, err = m.wordCount(string(data), args.Workers)
	case "sum":
		result, err = m.sumNumbers(data, args.Workers)
	case "avg":
		result, err = m.avgNumbers(data, args.Workers)
	case "groupby":
		result, err = m.groupBy(data, args.GroupByKey, args.Workers)
	default:
		return "", fmt.Errorf("unknown operation: %s", args.Operation)
	}

	if err != nil {
		return "", fmt.Errorf("operation failed: %w", err)
	}

	outputPath := filepath.Join(args.OutputDir, args.OutputFile)
	outputData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	if err := os.WriteFile(outputPath, outputData, 0644); err != nil {
		return "", fmt.Errorf("failed to write output: %w", err)
	}

	return fmt.Sprintf("MapReduce completed: %s -> %s (operation: %s, workers: %d) at %s",
		args.InputFile, outputPath, args.Operation, args.Workers, time.Now().Format(time.RFC3339)), nil
}

func (m *MapReducePlugin) wordCount(text string, workers int) (map[string]int, error) {
	words := strings.Fields(text)
	chunkSize := len(words) / workers
	if chunkSize == 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	results := make([]map[string]int, workers)

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == workers-1 {
				end = len(words)
			}

			localCount := make(map[string]int)
			for j := start; j < end && j < len(words); j++ {
				word := strings.ToLower(strings.Trim(words[j], ".,!?;:"))
				localCount[word]++
			}
			results[workerID] = localCount
		}(i)
	}

	wg.Wait()

	// Reduce
	finalCount := make(map[string]int)
	for _, localCount := range results {
		for word, count := range localCount {
			finalCount[word] += count
		}
	}

	return finalCount, nil
}

func (m *MapReducePlugin) sumNumbers(data []byte, workers int) (float64, error) {
	var numbers []float64
	if err := json.Unmarshal(data, &numbers); err != nil {
		return 0, err
	}

	chunkSize := len(numbers) / workers
	if chunkSize == 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	sums := make([]float64, workers)

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == workers-1 {
				end = len(numbers)
			}

			localSum := 0.0
			for j := start; j < end && j < len(numbers); j++ {
				localSum += numbers[j]
			}
			sums[workerID] = localSum
		}(i)
	}

	wg.Wait()

	total := 0.0
	for _, s := range sums {
		total += s
	}

	return total, nil
}

func (m *MapReducePlugin) avgNumbers(data []byte, workers int) (float64, error) {
	var numbers []float64
	if err := json.Unmarshal(data, &numbers); err != nil {
		return 0, err
	}

	sum, err := m.sumNumbers(data, workers)
	if err != nil {
		return 0, err
	}

	return sum / float64(len(numbers)), nil
}

func (m *MapReducePlugin) groupBy(data []byte, key string, workers int) (map[string][]map[string]interface{}, error) {
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}

	result := make(map[string][]map[string]interface{})
	var mu sync.Mutex

	chunkSize := len(records) / workers
	if chunkSize == 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == workers-1 {
				end = len(records)
			}

			localGroups := make(map[string][]map[string]interface{})
			for j := start; j < end && j < len(records); j++ {
				if keyVal, ok := records[j][key]; ok {
					keyStr := fmt.Sprintf("%v", keyVal)
					localGroups[keyStr] = append(localGroups[keyStr], records[j])
				}
			}

			mu.Lock()
			for k, v := range localGroups {
				result[k] = append(result[k], v...)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	return result, nil
}

type RandomImageRetrieverPlugin struct{}

type RandomImageRetrieverArgs struct {
	SourceDir  string   `json:"sourceDir"`  // Directory to search for images
	Count      int      `json:"count"`      // Number of random images to retrieve
	CopyTo     string   `json:"copyTo"`     // Directory to copy selected images
	Extensions []string `json:"extensions"` // Image extensions to look for
	OutputFile string   `json:"outputFile"` // JSON file with image paths
}

func (r *RandomImageRetrieverPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "image.RandomRetriever",
		Description: "Retrieves random images from a directory",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"sourceDir": {"type": "string", "default": "."},
				"count": {"type": "integer", "minimum": 1, "maximum": 100, "default": 5},
				"copyTo": {"type": "string", "default": "random_images"},
				"extensions": {"type": "array", "items": {"type": "string"}, "default": [".png", ".jpg", ".jpeg", ".gif"]},
				"outputFile": {"type": "string", "default": "random_images.json"}
			},
			"required": ["sourceDir", "count"]
		}`),
		FormSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"sourceDir": {"type": "string", "title": "Source Directory"},
				"count": {"type": "integer", "title": "Number of Images"},
				"copyTo": {"type": "string", "title": "Copy To Directory"},
				"extensions": {"type": "array", "title": "File Extensions"},
				"outputFile": {"type": "string", "title": "Output JSON File"}
			}
		}`),
	}
}

func (r *RandomImageRetrieverPlugin) Execute(ctx context.Context, args RandomImageRetrieverArgs) (string, error) {
	if err := os.MkdirAll(args.CopyTo, 0755); err != nil {
		return "", fmt.Errorf("failed to create copy directory: %w", err)
	}

	var images []string
	err := filepath.Walk(args.SourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			ext := strings.ToLower(filepath.Ext(path))
			for _, validExt := range args.Extensions {
				if ext == validExt {
					images = append(images, path)
					break
				}
			}
		}
		return nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(images) == 0 {
		return "", fmt.Errorf("no images found in %s", args.SourceDir)
	}

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	count := args.Count
	if count > len(images) {
		count = len(images)
	}

	rng.Shuffle(len(images), func(i, j int) {
		images[i], images[j] = images[j], images[i]
	})

	selectedImages := images[:count]
	copiedImages := make([]string, 0, count)

	for i, imgPath := range selectedImages {
		destName := fmt.Sprintf("random_%d_%s", i, filepath.Base(imgPath))
		destPath := filepath.Join(args.CopyTo, destName)

		if err := r.copyFile(imgPath, destPath); err != nil {
			return "", fmt.Errorf("failed to copy %s: %w", imgPath, err)
		}

		copiedImages = append(copiedImages, destPath)
	}

	outputPath := filepath.Join(args.CopyTo, args.OutputFile)
	outputData, err := json.MarshalIndent(map[string]interface{}{
		"source_dir":     args.SourceDir,
		"total_found":    len(images),
		"selected_count": count,
		"images":         copiedImages,
		"timestamp":      time.Now().Format(time.RFC3339),
	}, "", "  ")

	if err != nil {
		return "", fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := os.WriteFile(outputPath, outputData, 0644); err != nil {
		return "", fmt.Errorf("failed to write output file: %w", err)
	}

	return fmt.Sprintf("Retrieved %d random images from %s to %s (found %d total) at %s",
		count, args.SourceDir, args.CopyTo, len(images), time.Now().Format(time.RFC3339)), nil
}

func (r *RandomImageRetrieverPlugin) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

type FileZipperPlugin struct{}

type FileZipperArgs struct {
	SourcePaths []string `json:"sourcePaths"` // Files/directories to zip
	OutputFile  string   `json:"outputFile"`  // Output zip file name
	OutputDir   string   `json:"outputDir"`   // Directory for output
	Compression int      `json:"compression"` // 0-9, 0=no compression, 9=max
	IncludeRoot bool     `json:"includeRoot"` // Include root directory name
}

func (z *FileZipperPlugin) Meta() nexus.PluginMeta {
	return nexus.PluginMeta{
		Name:        "file.Zipper",
		Description: "Creates zip archives from files and directories",
		Version:     1,
		ArgsSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"sourcePaths": {"type": "array", "items": {"type": "string"}},
				"outputFile": {"type": "string", "default": "archive.zip"},
				"outputDir": {"type": "string", "default": "zip_outputs"},
				"compression": {"type": "integer", "minimum": 0, "maximum": 9, "default": 6},
				"includeRoot": {"type": "boolean", "default": false}
			},
			"required": ["sourcePaths", "outputFile"]
		}`),
		FormSchemaJSON: json.RawMessage(`{
			"type": "object",
			"properties": {
				"sourcePaths": {"type": "array", "title": "Source Paths", "items": {"type": "string"}},
				"outputFile": {"type": "string", "title": "Output Zip File"},
				"outputDir": {"type": "string", "title": "Output Directory"},
				"compression": {"type": "integer", "title": "Compression Level (0-9)"},
				"includeRoot": {"type": "boolean", "title": "Include Root Directory"}
			}
		}`),
	}
}

func (z *FileZipperPlugin) Execute(ctx context.Context, args FileZipperArgs) (string, error) {
	if err := os.MkdirAll(args.OutputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	outputPath := filepath.Join(args.OutputDir, args.OutputFile)
	zipFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to create zip file: %w", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	filesAdded := 0
	totalSize := int64(0)

	for _, sourcePath := range args.SourcePaths {
		info, err := os.Stat(sourcePath)
		if err != nil {
			return "", fmt.Errorf("failed to stat %s: %w", sourcePath, err)
		}

		if info.IsDir() {
			err = filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				relPath, err := filepath.Rel(sourcePath, path)
				if err != nil {
					return err
				}

				archivePath := relPath
				if args.IncludeRoot {
					archivePath = filepath.Join(filepath.Base(sourcePath), relPath)
				}

				if info.IsDir() {
					return nil
				}

				if err := z.addFileToZip(zipWriter, path, archivePath, args.Compression); err != nil {
					return err
				}

				filesAdded++
				totalSize += info.Size()
				return nil
			})

			if err != nil {
				return "", fmt.Errorf("failed to walk directory %s: %w", sourcePath, err)
			}
		} else {
			archivePath := filepath.Base(sourcePath)
			if err := z.addFileToZip(zipWriter, sourcePath, archivePath, args.Compression); err != nil {
				return "", fmt.Errorf("failed to add file %s: %w", sourcePath, err)
			}
			filesAdded++
			totalSize += info.Size()
		}
	}

	return fmt.Sprintf("Created zip archive: %s (%d files, %.2f MB) at %s",
		outputPath, filesAdded, float64(totalSize)/(1024*1024), time.Now().Format(time.RFC3339)), nil
}

func (z *FileZipperPlugin) addFileToZip(zipWriter *zip.Writer, filePath, archivePath string, compression int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	header.Name = archivePath
	header.Method = zip.Deflate

	if compression == 0 {
		header.Method = zip.Store
	}

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, file)
	return err
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type MockRedisTaskStateQueue struct {
	client   *redis.Client
	queueKey string
}

func NewMockRedisTaskStateQueue(client *redis.Client, queueKey string) *MockRedisTaskStateQueue {
	return &MockRedisTaskStateQueue{
		client:   client,
		queueKey: queueKey,
	}
}

func (r *MockRedisTaskStateQueue) PushNewTaskState(state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal task state: %s", err)
	}
	err = r.client.RPush(context.Background(), r.queueKey, string(data)).Err()
	if err != nil {
		return fmt.Errorf("failed to push task state to Redis: %s", err)
	}
	return nil
}

func (r *MockRedisTaskStateQueue) PullNewTaskState(ctx context.Context) (any, error) {
	result, err := r.client.LPop(ctx, r.queueKey).Result()
	if err != nil {
		return nil, err
	}
	if result == "" {
		return nexus.TaskState{}, fmt.Errorf("result is empty")
	}
	var state nexus.TaskState
	err = json.Unmarshal([]byte(result), &state)
	return state, err
}

func (r *MockRedisTaskStateQueue) IsEmpty(ctx context.Context) bool {
	length, err := r.client.LLen(ctx, r.queueKey).Result()
	if err != nil {
		return false
	}
	return length == 0
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to Redis")
	}

	mongoClient, err := mongo.Connect(
		ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to MongoDB")
	}
	defer mongoClient.Disconnect(ctx)

	if err := mongoClient.Ping(ctx, nil); err != nil {
		logger.Fatal().Err(err).Msg("Failed to ping MongoDB")
	}

	plugins := map[string]nexus.Plugin{
		"dummy.Dummy":     &DummyPlugin{},
		"image.Generator": &ImageGeneratorPlugin{},
		"pattern.Drawer":  &PatternDrawerPlugin{},
		"data.MapReduce":  &MapReducePlugin{},
		// "image.RandomRetriever": &RandomImageRetrieverPlugin{},
		// "file.Zipper":           &FileZipperPlugin{},
	}

	queueName := "core_stress_test_queue"
	taskStateQueue := NewMockRedisTaskStateQueue(redisClient, queueName)

	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		logger.Warn().Err(err).Msg("Failed to flush Redis DB")
	}

	watcherCtx, watcherCancel := context.WithCancel(ctx)
	defer watcherCancel()

	watcherArgs := nexus.NexusCoreBackendArgs{
		Redis: nexus.RedisArgs{
			Url: "localhost:6379",
			Db:  0,
		},
		MongoDbClient:          mongoClient,
		Plugins:                plugins,
		Logger:                 logger,
		MaxPluginWorkers:       8,
		TaskStateQueue:         taskStateQueue,
		MaxFlowQueueLength:     20000,
		ScanAndFixFlowInterval: 2 * time.Second,
		StreamCapacity:         1000,
	}

	watcherCore, err := nexus.NewNexusCore(watcherCtx, watcherArgs)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize watcher NexusCore")
	}
	defer watcherCore.Shutdown()

	go watcherCore.Run(8)

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n🚀 Starting NexusCore Pipeline Demonstration")
	fmt.Println("============================================")

	prepareSampleDataFile := func() string {
		dataDir := "sample_data"
		os.MkdirAll(dataDir, 0755)

		numbers := make([]float64, 100)
		source := rand.NewSource(time.Now().UnixNano())
		r := rand.New(source)
		for i := range numbers {
			numbers[i] = float64(r.Intn(1000))
		}

		data, _ := json.MarshalIndent(numbers, "", "  ")
		filename := filepath.Join(dataDir, "numbers.json")
		os.WriteFile(filename, data, 0644)

		return filename
	}

	dataFile := prepareSampleDataFile()
	logger.Info().Msgf("Created sample data file: %s", dataFile)

	backoffStrategy := backoff.NewExponentialBackOff()
	backoffStrategy.MaxElapsedTime = 30 * time.Second

	fmt.Println("📷 Phase 1: Generating images...")
	imagePatterns := []string{"gradient", "circles", "squares", "stripes"}
	colorSchemes := []string{"blue", "red", "green", "rainbow", "random"}

	for i := range 10 {
		event := nexus.EventDetail{
			DelegationType: "image.Generator",
			Payload: fmt.Sprintf(`{
				"width": %d,
				"height": %d,
				"pattern": "%s",
				"colorScheme": "%s",
				"filename": "pipeline_image_%d.png"
			}`, 400+i*20, 300+i*15,
				imagePatterns[i%len(imagePatterns)],
				colorSchemes[i%len(colorSchemes)], i),
			MaxAttempts: 3,
			Attempts:    0,
		}
		watcherCore.SubmitEvent(event, backoffStrategy)
	}

	fmt.Println("🎨 Phase 2: Creating pattern drawings...")
	patterns := [][]string{
		{"spiral", "waves"},
		{"checkerboard", "mandala"},
		{"hexagons", "fractals"},
	}

	for i, patternSet := range patterns {
		patternJSON, _ := json.Marshal(patternSet)
		colorsJSON, _ := json.Marshal([]string{
			"#FF5733", "#33FF57",
			"#3357FF", "#FFD700",
		})

		event := nexus.EventDetail{
			DelegationType: "pattern.Drawer",
			Payload: fmt.Sprintf(`{
				"width": 800,
				"height": 800,
				"patterns": %s,
				"colors": %s,
				"filename": "pattern_%d.png",
				"outputDir": "pattern_outputs",
				"complexity": %d
			}`, patternJSON, colorsJSON, i, 5+i),
			MaxAttempts: 3,
			Attempts:    0,
		}
		watcherCore.SubmitEvent(event, backoffStrategy)
	}

	time.Sleep(3 * time.Second)

	fmt.Println("📊 Phase 3: Processing data with MapReduce...")
	mapReduceOps := []string{"sum", "avg", "wordcount"}

	for _, op := range mapReduceOps {
		event := nexus.EventDetail{
			DelegationType: "data.MapReduce",
			Payload: fmt.Sprintf(`{
				"inputFile": "%s",
				"operation": "%s",
				"outputFile": "result_%s.json",
				"outputDir": "mapreduce_outputs",
				"workers": 4
			}`, dataFile, op, op),
			MaxAttempts: 3,
			Attempts:    0,
		}
		watcherCore.SubmitEvent(event, backoffStrategy)
	}

	time.Sleep(2 * time.Second)

	fmt.Println("🎲 Phase 4: Retrieving random images...")
	event := nexus.EventDetail{
		DelegationType: "image.RandomRetriever",
		Payload: `{
			"sourceDir": "generated_images",
			"count": 5,
			"copyTo": "random_selected_images",
			"extensions": [".png"],
			"outputFile": "selected_images.json"
		}`,
		MaxAttempts: 3,
		Attempts:    0,
	}
	watcherCore.SubmitEvent(event, backoffStrategy)
	time.Sleep(2 * time.Second)

	fmt.Println("📦 Phase 5: Creating zip archives...")
	zipConfigs := []struct {
		name   string
		paths  []string
		output string
	}{
		{
			name:   "images_archive",
			paths:  []string{"generated_images"},
			output: "generated_images.zip",
		},
		{
			name:   "patterns_archive",
			paths:  []string{"pattern_outputs"},
			output: "pattern_outputs.zip",
		},
		{
			name:   "mapreduce_archive",
			paths:  []string{"mapreduce_outputs"},
			output: "mapreduce_results.zip",
		},
		{
			name: "complete_archive",
			paths: []string{
				"generated_images", "pattern_outputs",
				"mapreduce_outputs", "random_selected_images",
			},
			output: "complete_pipeline.zip",
		},
	}

	for _, cfg := range zipConfigs {
		pathsJSON, _ := json.Marshal(cfg.paths)
		event := nexus.EventDetail{
			DelegationType: "file.Zipper",
			Payload: fmt.Sprintf(`{
				"sourcePaths": %s,
				"outputFile": "%s",
				"outputDir": "zip_outputs",
				"compression": 6,
				"includeRoot": true
			}`, pathsJSON, cfg.output),
			MaxAttempts: 3,
			Attempts:    0,
		}
		watcherCore.SubmitEvent(event, backoffStrategy)
	}

	fmt.Println("\n⏳ Waiting for all pipeline tasks to complete...")
	fmt.Println("💡 Press Ctrl+C to quit early if needed")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	maxWaitTime := 3 * time.Minute
	checkInterval := 2 * time.Second
	totalWaitTime := time.Duration(0)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	quitEarly := false

	for {
		select {
		case <-sigChan:
			fmt.Println("\n\n⚠️  Interrupt signal received. Shutting down gracefully...")
			quitEarly = true
			goto cleanup

		case <-ticker.C:
			patterns := []string{"task_*"}
			totalKeys := 0

			for _, pattern := range patterns {
				keys, err := redisClient.Keys(ctx, pattern).Result()
				if err != nil {
					logger.Error().Err(err).Msgf("Failed to check keys for pattern %s", pattern)
					continue
				}
				totalKeys += len(keys)
			}

			queueLen, err := redisClient.LLen(ctx, queueName).Result()
			if err != nil {
				logger.Error().Err(err).Msg("Failed to check queue length")
			} else {
				totalKeys += int(queueLen)
			}

			if totalKeys == 0 {
				fmt.Println("✅ All pipeline tasks completed! Queue is empty.")
				goto cleanup
			}

			fmt.Printf("⏳ Processing... %d items remaining\n", totalKeys)
			totalWaitTime += checkInterval

			if totalWaitTime >= maxWaitTime {
				fmt.Printf("⚠️  Timeout reached after %.0f minutes. Some tasks may still be processing.\n", maxWaitTime.Minutes())
				goto cleanup
			}
		}
	}

cleanup:
	if quitEarly {
		queueLen, _ := redisClient.LLen(ctx, queueName).Result()
		if queueLen > 0 {
			fmt.Printf("⚠️  Warning: %d tasks still in queue\n", queueLen)
		}
	}

	fmt.Println("\n📋 Pipeline Summary")
	fmt.Println("===================")

	checkDir := func(dir string) int {
		count := 0
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				count++
			}
			return nil
		})
		return count
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Phase", "Plugin", "Output", "Files Created"})
	table.Append([]string{"1", "Image Generator", "generated_images/", fmt.Sprintf("%d", checkDir("generated_images"))})
	table.Append([]string{"2", "Pattern Drawer", "pattern_outputs/", fmt.Sprintf("%d", checkDir("pattern_outputs"))})
	table.Append([]string{"3", "MapReduce", "mapreduce_outputs/", fmt.Sprintf("%d", checkDir("mapreduce_outputs"))})
	table.Append([]string{"4", "Random Retriever", "random_selected_images/", fmt.Sprintf("%d", checkDir("random_selected_images"))})
	table.Append([]string{"5", "File Zipper", "zip_outputs/", fmt.Sprintf("%d", checkDir("zip_outputs"))})
	table.Render()

	fmt.Println("\n🎉 Pipeline demonstration complete!")
	fmt.Println("\nGenerated artifacts:")
	fmt.Println("  • generated_images/       - Generated images")
	fmt.Println("  • pattern_outputs/        - Pattern drawings")
	fmt.Println("  • mapreduce_outputs/      - Data processing results")
	fmt.Println("  • random_selected_images/ - Randomly selected images")
	fmt.Println("  • zip_outputs/            - Compressed archives")

}
