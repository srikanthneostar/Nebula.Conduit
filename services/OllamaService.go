package services

import (
	"context"

	"github.com/parakeet-nest/parakeet/completion"
	"github.com/parakeet-nest/parakeet/llm"
)

type OllamaService struct {
	SystemContent string
	Model         string
	OllamaUrl     string
}

// GetResponse generates a response using the Ollama language model based on provided context and question.
// It takes a context.Context, a slice of context strings, and a question string as input.
// The function combines system content, context messages, and the user question into a chat query,
// then streams the response from the Ollama service.
//
// Parameters:
//   - ctx: The context.Context for the request
//   - contexts: A slice of strings containing contextual information
//   - question: The user's question or prompt
//
// Returns:
//   - string: The generated response from the language model
//   - error: An error if the request fails, nil otherwise
func (s *OllamaService) GetResponse(ctx context.Context, contexts []string, question string) (string, error) {
	options := llm.Options{
		Temperature:   0.0,
		RepeatLastN:   2,
		RepeatPenalty: 1.5,
	}

	contextMessages := make([]llm.Message, len(contexts))
	for i, context := range contexts {
		contextMessages[i] = llm.Message{
			Role:    "system",
			Content: context,
		}
	}

	messages := append([]llm.Message{{Role: "system", Content: s.SystemContent}}, contextMessages...)
	messages = append(messages, llm.Message{Role: "user", Content: question})

	query := llm.Query{
		Model:    s.Model,
		Messages: messages,
		Options:  options,
	}

	response := ""
	_, err := completion.ChatStream(s.OllamaUrl, query,
		func(answer llm.Answer) error {
			response += answer.Message.Content
			return nil
		})

	if err != nil {
		return "", err
	}

	return response, nil
}
