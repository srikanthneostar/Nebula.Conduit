package embedding

import (
	"bufio"
	"fmt"
	"os/exec"
)

func EmbedMain(script_name string, query string) ([]float32, error) {
	cmd := exec.Command("python", script_name, query)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Read and parse the output
	var embeddings []float32
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		// Assume each line represents a float32 number
		var value float32
		if _, err := fmt.Sscanf(line, "%f", &value); err == nil {
			embeddings = append(embeddings, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Wait for the command to complete
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return embeddings, nil
}
