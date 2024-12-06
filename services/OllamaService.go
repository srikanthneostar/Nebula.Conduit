package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type OllamaService struct {
	baseURL string
}

type OllamaRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OllamaResponse struct {
	Message Message `json:"message"`
}

func NewOllamaService(baseURL string) *OllamaService {
	return &OllamaService{
		baseURL: baseURL,
	}
}

// GenerateResponse generates a response using Ollama with system context, user question and data
func (o *OllamaService) GenerateResponse(model string, systemContext string, userQuestion string, data string) (string, error) {
	messages := []Message{
		{
			Role:    "system",
			Content: systemContext,
		},
		{
			Role:    "user",
			Content: fmt.Sprintf("Question: %s\nContext: %s", userQuestion, data),
		},
	}

	request := OllamaRequest{
		Model:    model,
		Messages: messages,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("error marshaling request: %w", err)
	}

	resp, err := http.Post(o.baseURL+"/api/chat", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("error making request to Ollama: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	var ollamaResp OllamaResponse
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return "", fmt.Errorf("error unmarshaling response: %w", err)
	}

	return ollamaResp.Message.Content, nil
}
