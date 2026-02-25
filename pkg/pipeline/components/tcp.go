package components

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// TCPReadComponent reads data from TCP connections (server or client mode)
type TCPReadComponent struct {
	config            pipeline.ComponentConfig
	mode              string // "server" or "client"
	host              string
	port              int
	encoding          string
	connectionTimeout time.Duration
	maxConnections    int
	logger            zerolog.Logger
}

// NewTCPReadComponent creates a new TCP Read component
func NewTCPReadComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract mode from parameters
	mode, ok := config.Parameters["mode"].(string)
	if !ok || (mode != "server" && mode != "client") {
		return nil, fmt.Errorf("mode parameter is required and must be 'server' or 'client'")
	}

	// Extract host from parameters
	host, ok := config.Parameters["host"].(string)
	if !ok || host == "" {
		return nil, fmt.Errorf("host parameter is required and must be a string")
	}

	// Extract port from parameters
	var port int
	switch v := config.Parameters["port"].(type) {
	case float64:
		port = int(v)
	case int:
		port = v
	default:
		return nil, fmt.Errorf("port parameter is required and must be a number")
	}

	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("port must be between 1 and 65535")
	}

	// Extract encoding (optional, defaults to utf-8)
	encoding := "utf-8"
	if encodingParam, ok := config.Parameters["encoding"].(string); ok && encodingParam != "" {
		encoding = encodingParam
	}

	// Extract connection timeout (optional, defaults to 30s)
	connectionTimeout := 30 * time.Second
	if timeoutParam, ok := config.Parameters["connection_timeout"].(string); ok {
		parsedTimeout, err := time.ParseDuration(timeoutParam)
		if err != nil {
			return nil, fmt.Errorf("invalid connection_timeout format: %w", err)
		}
		connectionTimeout = parsedTimeout
	}

	// Extract max connections for server mode (optional, defaults to 10)
	maxConnections := 10
	if maxConnParam, ok := config.Parameters["max_connections"].(float64); ok {
		maxConnections = int(maxConnParam)
	} else if maxConnParam, ok := config.Parameters["max_connections"].(int); ok {
		maxConnections = maxConnParam
	}

	return &TCPReadComponent{
		config:            config,
		mode:              mode,
		host:              host,
		port:              port,
		encoding:          encoding,
		connectionTimeout: connectionTimeout,
		maxConnections:    maxConnections,
		logger:            logger.InitLogger(),
	}, nil
}

// Execute runs the TCP Read component logic
func (t *TCPReadComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		if t.mode == "server" {
			t.runServer(ctx, output)
		} else {
			t.runClient(ctx, output)
		}
	}()

	return output, nil
}

// runServer runs TCP server mode
func (t *TCPReadComponent) runServer(ctx context.Context, output chan<- pipeline.Data) {
	address := fmt.Sprintf("%s:%d", t.host, t.port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.logger.Error().Err(err).Str("address", address).Msg("Failed to start TCP server")
		return
	}
	defer listener.Close()

	t.logger.Info().Str("address", address).Msg("TCP server listening")

	// Channel to limit concurrent connections
	connSemaphore := make(chan struct{}, t.maxConnections)

	// Accept connections until context is cancelled
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				t.logger.Error().Err(err).Msg("Failed to accept connection")
				continue
			}
		}

		// Acquire semaphore slot
		select {
		case connSemaphore <- struct{}{}:
			go func(c net.Conn) {
				defer func() { <-connSemaphore }()
				t.handleConnection(ctx, c, output)
			}(conn)
		case <-ctx.Done():
			conn.Close()
			return
		}
	}
}

// runClient runs TCP client mode
func (t *TCPReadComponent) runClient(ctx context.Context, output chan<- pipeline.Data) {
	address := fmt.Sprintf("%s:%d", t.host, t.port)

	// Connect with timeout
	dialer := net.Dialer{Timeout: t.connectionTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		t.logger.Error().Err(err).Str("address", address).Msg("Failed to connect to TCP server")
		return
	}
	defer conn.Close()

	t.logger.Info().Str("address", address).Msg("Connected to TCP server")

	t.handleConnection(ctx, conn, output)
}

// handleConnection reads data from a TCP connection
func (t *TCPReadComponent) handleConnection(ctx context.Context, conn net.Conn, output chan<- pipeline.Data) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(t.connectionTimeout))

		// Read data (line by line)
		data, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				t.logger.Debug().Msg("Connection closed by peer")
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout is expected, continue
				continue
			}
			t.logger.Error().Err(err).Msg("Failed to read from connection")
			return
		}

		// Create data payload
		pipelineData := pipeline.Data{
			Payload:   data,
			Metadata:  make(map[string]string),
			Timestamp: time.Now(),
			TraceID:   t.config.ID,
		}

		// Add metadata
		pipelineData.Metadata["source"] = "tcp"
		pipelineData.Metadata["mode"] = t.mode
		pipelineData.Metadata["remote_addr"] = conn.RemoteAddr().String()
		pipelineData.Metadata["encoding"] = t.encoding

		// Send data to output channel
		select {
		case output <- pipelineData:
		case <-ctx.Done():
			return
		}
	}
}

// Validate checks if the component configuration is valid
func (t *TCPReadComponent) Validate() error {
	if t.mode != "server" && t.mode != "client" {
		return fmt.Errorf("mode must be 'server' or 'client'")
	}

	if t.host == "" {
		return fmt.Errorf("host is required")
	}

	if t.port <= 0 || t.port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	return nil
}

// Type returns the component type identifier
func (t *TCPReadComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeTCPRead
}

// ID returns the unique component instance identifier
func (t *TCPReadComponent) ID() string {
	return t.config.ID
}

// Config returns the component configuration
func (t *TCPReadComponent) Config() pipeline.ComponentConfig {
	return t.config
}

// TCPWriteComponent writes data to TCP connections (server or client mode)
type TCPWriteComponent struct {
	config            pipeline.ComponentConfig
	mode              string // "server" or "client"
	host              string
	port              int
	encoding          string
	connectionTimeout time.Duration
	maxConnections    int
	logger            zerolog.Logger
}

// NewTCPWriteComponent creates a new TCP Write component
func NewTCPWriteComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract mode from parameters
	mode, ok := config.Parameters["mode"].(string)
	if !ok || (mode != "server" && mode != "client") {
		return nil, fmt.Errorf("mode parameter is required and must be 'server' or 'client'")
	}

	// Extract host from parameters
	host, ok := config.Parameters["host"].(string)
	if !ok || host == "" {
		return nil, fmt.Errorf("host parameter is required and must be a string")
	}

	// Extract port from parameters
	var port int
	switch v := config.Parameters["port"].(type) {
	case float64:
		port = int(v)
	case int:
		port = v
	default:
		return nil, fmt.Errorf("port parameter is required and must be a number")
	}

	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("port must be between 1 and 65535")
	}

	// Extract encoding (optional, defaults to utf-8)
	encoding := "utf-8"
	if encodingParam, ok := config.Parameters["encoding"].(string); ok && encodingParam != "" {
		encoding = encodingParam
	}

	// Extract connection timeout (optional, defaults to 30s)
	connectionTimeout := 30 * time.Second
	if timeoutParam, ok := config.Parameters["connection_timeout"].(string); ok {
		parsedTimeout, err := time.ParseDuration(timeoutParam)
		if err != nil {
			return nil, fmt.Errorf("invalid connection_timeout format: %w", err)
		}
		connectionTimeout = parsedTimeout
	}

	// Extract max connections for server mode (optional, defaults to 10)
	maxConnections := 10
	if maxConnParam, ok := config.Parameters["max_connections"].(float64); ok {
		maxConnections = int(maxConnParam)
	} else if maxConnParam, ok := config.Parameters["max_connections"].(int); ok {
		maxConnections = maxConnParam
	}

	return &TCPWriteComponent{
		config:            config,
		mode:              mode,
		host:              host,
		port:              port,
		encoding:          encoding,
		connectionTimeout: connectionTimeout,
		maxConnections:    maxConnections,
		logger:            logger.InitLogger(),
	}, nil
}

// Execute runs the TCP Write component logic
func (tw *TCPWriteComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		if tw.mode == "server" {
			tw.runServer(ctx, input)
		} else {
			tw.runClient(ctx, input)
		}
	}()

	return output, nil
}

// runServer runs TCP server mode for writing
func (tw *TCPWriteComponent) runServer(ctx context.Context, input <-chan pipeline.Data) {
	address := fmt.Sprintf("%s:%d", tw.host, tw.port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		tw.logger.Error().Err(err).Str("address", address).Msg("Failed to start TCP server")
		return
	}
	defer listener.Close()

	tw.logger.Info().Str("address", address).Msg("TCP write server listening")

	// Accept one connection and write data to it
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	conn, err := listener.Accept()
	if err != nil {
		tw.logger.Error().Err(err).Msg("Failed to accept connection")
		return
	}
	defer conn.Close()

	tw.writeToConnection(ctx, conn, input)
}

// runClient runs TCP client mode for writing
func (tw *TCPWriteComponent) runClient(ctx context.Context, input <-chan pipeline.Data) {
	address := fmt.Sprintf("%s:%d", tw.host, tw.port)

	// Connect with timeout
	dialer := net.Dialer{Timeout: tw.connectionTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		tw.logger.Error().Err(err).Str("address", address).Msg("Failed to connect to TCP server")
		return
	}
	defer conn.Close()

	tw.logger.Info().Str("address", address).Msg("Connected to TCP server for writing")

	tw.writeToConnection(ctx, conn, input)
}

// writeToConnection writes data from input channel to TCP connection
func (tw *TCPWriteComponent) writeToConnection(ctx context.Context, conn net.Conn, input <-chan pipeline.Data) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-input:
			if !ok {
				// Input channel closed
				return
			}

			// Convert payload to bytes
			var bytes []byte
			switch v := data.Payload.(type) {
			case []byte:
				bytes = v
			case string:
				bytes = []byte(v)
			default:
				tw.logger.Error().Msgf("Unsupported payload type: %T", data.Payload)
				if !tw.config.ContinueOnError {
					return
				}
				continue
			}

			// Set write deadline
			conn.SetWriteDeadline(time.Now().Add(tw.connectionTimeout))

			// Write data to connection
			_, err := conn.Write(bytes)
			if err != nil {
				tw.logger.Error().Err(err).Msg("Failed to write to connection")
				if !tw.config.ContinueOnError {
					return
				}
				continue
			}

			tw.logger.Debug().Int("bytes", len(bytes)).Msg("Data written to TCP connection")
		}
	}
}

// Validate checks if the component configuration is valid
func (tw *TCPWriteComponent) Validate() error {
	if tw.mode != "server" && tw.mode != "client" {
		return fmt.Errorf("mode must be 'server' or 'client'")
	}

	if tw.host == "" {
		return fmt.Errorf("host is required")
	}

	if tw.port <= 0 || tw.port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	return nil
}

// Type returns the component type identifier
func (tw *TCPWriteComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeTCPWrite
}

// ID returns the unique component instance identifier
func (tw *TCPWriteComponent) ID() string {
	return tw.config.ID
}

// Config returns the component configuration
func (tw *TCPWriteComponent) Config() pipeline.ComponentConfig {
	return tw.config
}
