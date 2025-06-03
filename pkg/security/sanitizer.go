package security

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ErrInvalidScriptName = errors.New("invalid script name")
	scriptNameRegex      = regexp.MustCompile(`^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9]+)?$`)
)

type ScriptValidator interface {
	Validate(script string, args []string) error
}

type scriptValidator struct{}

func NewScriptValidator() ScriptValidator {
	return &scriptValidator{}
}

func (v *scriptValidator) Validate(script string, args []string) error {
	if script == "" || strings.Contains(script, "..") || strings.Contains(script, "/") {
		return ErrInvalidScriptName
	}

	if !scriptNameRegex.MatchString(script) {
		return ErrInvalidScriptName
	}

	ext := filepath.Ext(script)
	if ext != "" && ext != ".py" && ext != ".sh" && ext != "" {
		return ErrInvalidScriptName
	}

	// Additional argument validation can be added here
	return nil
}
