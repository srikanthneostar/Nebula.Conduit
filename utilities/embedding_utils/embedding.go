package embedding

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
)

func GenerateEmbedding(query string) ([]float64, error) {
	_, b, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(b)
	appPath := filepath.Join(basePath, "app.py")
	output, err := runPythonScript(appPath, query)
	if err != nil {
		return nil, fmt.Errorf("error running Python script: %v", err)
	}

	var embedding []float64
	if err := json.Unmarshal([]byte(output), &embedding); err != nil {
		return nil, fmt.Errorf("error parsing Python output: %v", err)
	}
	return embedding, nil
}

func runPythonScript(appPath, query string) (string, error) {
	cmd := exec.Command("python", appPath, query)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("error running Python script: %v\nOutput: %s", err, string(output))
	}
	return string(output), nil
}
