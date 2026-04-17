package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// PipelineHandlers contains HTTP handlers for pipeline API endpoints
type PipelineHandlers struct {
	engine *PipelineEngine
}

func NewPipelineHandlers(engine *PipelineEngine) *PipelineHandlers {
	return &PipelineHandlers{engine: engine}
}

// CreatePipeline handles POST /api/v1/pipelines
func (h *PipelineHandlers) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload", "INVALID_REQUEST", nil)
		return
	}

	def := PipelineDefinition{
		ID:             uuid.New().String(),
		Name:           req.Name,
		Description:    req.Description,
		ExecutionMode:  req.ExecutionMode,
		CronExpression: req.CronExpression,
		Status:         req.Status,
		Components:     req.Components,
		Connections:    req.Connections,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if def.Status == "" {
		def.Status = PipelineStatusInactive
	}

	if err := h.engine.GetRepository().Create(def); err != nil {
		log.Error().Err(err).Str("name", req.Name).Msg("Failed to create pipeline")
		respondWithError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR", nil)
		return
	}

	// If pipeline is active and has a cron expression, schedule it immediately
	if def.Status == PipelineStatusActive && def.ExecutionMode == ExecutionModeScheduled && def.CronExpression != "" {
		if err := h.engine.GetScheduler().Schedule(def); err != nil {
			log.Error().Err(err).Str("pipeline_id", def.ID).Msg("Failed to schedule pipeline")
			// Don't fail the creation, just log the error
		} else {
			log.Info().Str("pipeline_id", def.ID).Str("cron", def.CronExpression).Msg("Pipeline scheduled")
		}
	}

	log.Info().Str("pipeline_id", def.ID).Str("name", def.Name).Msg("Pipeline created")
	respondWithJSON(w, http.StatusCreated, toPipelineResponse(def))
}

// GetPipeline handles GET /api/v1/pipelines/{id}
func (h *PipelineHandlers) GetPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	def, err := h.engine.GetRepository().Read(pipelineID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Pipeline not found", "NOT_FOUND", nil)
		return
	}

	respondWithJSON(w, http.StatusOK, toPipelineResponse(def))
}

// ListPipelines handles GET /api/v1/pipelines
func (h *PipelineHandlers) ListPipelines(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")

	var pipelines []PipelineDefinition
	var err error

	if status == string(PipelineStatusActive) {
		pipelines, err = h.engine.GetRepository().ListActive()
	} else {
		pipelines, err = h.engine.GetRepository().List()
	}

	if err != nil {
		log.Error().Err(err).Msg("Failed to list pipelines")
		respondWithError(w, http.StatusInternalServerError, "Failed to list pipelines", "INTERNAL_ERROR", nil)
		return
	}

	// Filter by status if provided and not already filtered
	if status != "" && status != string(PipelineStatusActive) {
		var filtered []PipelineDefinition
		for _, p := range pipelines {
			if string(p.Status) == status {
				filtered = append(filtered, p)
			}
		}
		pipelines = filtered
	}

	var responses []PipelineResponse
	for _, p := range pipelines {
		responses = append(responses, toPipelineResponse(p))
	}

	respondWithJSON(w, http.StatusOK, ListPipelinesResponse{
		Pipelines: responses,
		Total:     len(responses),
	})
}

// UpdatePipeline handles PUT /api/v1/pipelines/{id}
func (h *PipelineHandlers) UpdatePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	existing, err := h.engine.GetRepository().Read(pipelineID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Pipeline not found", "NOT_FOUND", nil)
		return
	}

	var req UpdatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload", "INVALID_REQUEST", nil)
		return
	}

	// Apply partial updates
	if req.Name != nil {
		existing.Name = *req.Name
	}
	if req.Description != nil {
		existing.Description = *req.Description
	}
	if req.ExecutionMode != nil {
		existing.ExecutionMode = *req.ExecutionMode
	}
	if req.CronExpression != nil {
		existing.CronExpression = *req.CronExpression
	}
	if req.Status != nil {
		existing.Status = *req.Status
	}
	if req.Components != nil {
		existing.Components = *req.Components
	}
	if req.Connections != nil {
		existing.Connections = *req.Connections
	}

	if err := h.engine.GetRepository().Update(existing); err != nil {
		log.Error().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to update pipeline")
		respondWithError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR", nil)
		return
	}

	// Handle scheduling changes
	if existing.ExecutionMode == ExecutionModeScheduled && existing.CronExpression != "" {
		if existing.Status == PipelineStatusActive {
			// Schedule or reschedule the pipeline
			if err := h.engine.GetScheduler().Schedule(existing); err != nil {
				log.Error().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to schedule pipeline")
			} else {
				log.Info().Str("pipeline_id", pipelineID).Str("cron", existing.CronExpression).Msg("Pipeline scheduled")
			}
		} else {
			// Unschedule if status changed to inactive
			if err := h.engine.GetScheduler().Unschedule(pipelineID); err != nil {
				log.Warn().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to unschedule pipeline")
			} else {
				log.Info().Str("pipeline_id", pipelineID).Msg("Pipeline unscheduled")
			}
		}
	}

	log.Info().Str("pipeline_id", pipelineID).Msg("Pipeline updated")
	respondWithJSON(w, http.StatusOK, toPipelineResponse(existing))
}

// DeletePipeline handles DELETE /api/v1/pipelines/{id}
func (h *PipelineHandlers) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	// Read pipeline to check execution mode
	pipeline, err := h.engine.GetRepository().Read(pipelineID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Pipeline not found", "NOT_FOUND", nil)
		return
	}

	// Unschedule if it's a scheduled pipeline
	if pipeline.ExecutionMode == ExecutionModeScheduled {
		if err := h.engine.GetScheduler().Unschedule(pipelineID); err != nil {
			log.Warn().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to unschedule pipeline")
		}
	}

	// Stop pipeline if running
	_ = h.engine.GetLifecycleManager().Stop(pipelineID)

	if err := h.engine.GetRepository().Delete(pipelineID); err != nil {
		respondWithError(w, http.StatusNotFound, "Pipeline not found", "NOT_FOUND", nil)
		return
	}

	log.Info().Str("pipeline_id", pipelineID).Msg("Pipeline deleted")
	respondWithJSON(w, http.StatusOK, MessageResponse{Message: "Pipeline deleted successfully"})
}

// TriggerPipeline handles POST /api/v1/pipelines/{id}/trigger
func (h *PipelineHandlers) TriggerPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	def, err := h.engine.GetRepository().Read(pipelineID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Pipeline not found", "NOT_FOUND", nil)
		return
	}

	instance, err := h.engine.GetExecutor().Execute(context.Background(), def)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to trigger pipeline")
		respondWithError(w, http.StatusInternalServerError, err.Error(), "EXECUTION_ERROR", nil)
		return
	}

	log.Info().Str("pipeline_id", pipelineID).Str("instance_id", instance.ID).Msg("Pipeline triggered")
	respondWithJSON(w, http.StatusOK, TriggerPipelineResponse{
		InstanceID: instance.ID,
		PipelineID: pipelineID,
		Status:     string(instance.Status),
		Message:    "Pipeline execution triggered successfully",
	})
}

// StopPipelineInstance handles POST /api/v1/pipelines/instances/{id}/stop
func (h *PipelineHandlers) StopPipelineInstance(w http.ResponseWriter, r *http.Request) {
	instanceID := chi.URLParam(r, "id")
	if instanceID == "" {
		respondWithError(w, http.StatusBadRequest, "Instance ID is required", "INVALID_REQUEST", nil)
		return
	}

	if err := h.engine.GetExecutor().Stop(instanceID); err != nil {
		respondWithError(w, http.StatusNotFound, err.Error(), "NOT_FOUND", nil)
		return
	}

	log.Info().Str("instance_id", instanceID).Msg("Pipeline instance stopped")
	respondWithJSON(w, http.StatusOK, StopInstanceResponse{
		InstanceID: instanceID,
		Status:     string(InstanceStatusStopped),
		Message:    "Pipeline instance stopped successfully",
	})
}

// GetInstanceStatus handles GET /api/v1/pipelines/instances/{id}
func (h *PipelineHandlers) GetInstanceStatus(w http.ResponseWriter, r *http.Request) {
	instanceID := chi.URLParam(r, "id")
	if instanceID == "" {
		respondWithError(w, http.StatusBadRequest, "Instance ID is required", "INVALID_REQUEST", nil)
		return
	}

	status, err := h.engine.GetExecutor().GetStatus(instanceID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, err.Error(), "NOT_FOUND", nil)
		return
	}

	respondWithJSON(w, http.StatusOK, GetInstanceStatusResponse{
		InstanceID: status.InstanceID,
		PipelineID: status.PipelineID,
		Status:     status.Status,
		StartedAt:  status.StartedAt.Format(time.RFC3339),
		Components: status.Components,
	})
}

// ListRunningInstances handles GET /api/v1/pipelines/instances
func (h *PipelineHandlers) ListRunningInstances(w http.ResponseWriter, r *http.Request) {
	instances := h.engine.GetLifecycleManager().GetRunningInstances()

	var responses []GetInstanceStatusResponse
	for _, inst := range instances {
		responses = append(responses, GetInstanceStatusResponse{
			InstanceID: inst.ID,
			PipelineID: inst.PipelineID,
			Status:     inst.Status,
			StartedAt:  inst.StartedAt.Format(time.RFC3339),
		})
	}

	respondWithJSON(w, http.StatusOK, ListRunningInstancesResponse{
		Instances: responses,
		Total:     len(responses),
	})
}

// GetExecutionHistory handles GET /api/v1/pipelines/{id}/executions
func (h *PipelineHandlers) GetExecutionHistory(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	page := 1
	pageSize := 20

	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if pageSizeStr := r.URL.Query().Get("page_size"); pageSizeStr != "" {
		if ps, err := strconv.Atoi(pageSizeStr); err == nil && ps > 0 && ps <= 100 {
			pageSize = ps
		}
	}

	offset := (page - 1) * pageSize
	records, total, err := h.engine.GetRecorder().GetExecutionHistory(r.Context(), pipelineID, pageSize, offset)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to get execution history")
		respondWithError(w, http.StatusInternalServerError, "Failed to get execution history", "INTERNAL_ERROR", nil)
		return
	}

	respondWithJSON(w, http.StatusOK, GetExecutionHistoryResponse{
		Executions: records,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
	})
}

// GetExecutionDetails handles GET /api/v1/pipelines/executions/{id}
func (h *PipelineHandlers) GetExecutionDetails(w http.ResponseWriter, r *http.Request) {
	executionID := chi.URLParam(r, "id")
	if executionID == "" {
		respondWithError(w, http.StatusBadRequest, "Execution ID is required", "INVALID_REQUEST", nil)
		return
	}

	record, err := h.engine.GetRecorder().GetExecutionDetails(r.Context(), executionID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Execution not found", "NOT_FOUND", nil)
		return
	}

	respondWithJSON(w, http.StatusOK, GetExecutionDetailsResponse{ExecutionRecord: *record})
}

// Helper functions

func respondWithError(w http.ResponseWriter, code int, message string, errorCode string, details map[string]interface{}) {
	respondWithJSON(w, code, ErrorResponse{
		Error:   message,
		Code:    errorCode,
		Details: details,
	})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal response")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func toPipelineResponse(def PipelineDefinition) PipelineResponse {
	return PipelineResponse{
		ID:             def.ID,
		Name:           def.Name,
		Description:    def.Description,
		ExecutionMode:  def.ExecutionMode,
		CronExpression: def.CronExpression,
		Status:         def.Status,
		Components:     def.Components,
		Connections:    def.Connections,
		CreatedAt:      def.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      def.UpdatedAt.Format(time.RFC3339),
	}
}

// ExportPipelines handles GET /api/v1/pipelines/export
// Exports all pipelines (or filtered by ?ids=id1,id2) in a portable JSON format.
func (h *PipelineHandlers) ExportPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.engine.GetRepository().List()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list pipelines for export")
		respondWithError(w, http.StatusInternalServerError, "Failed to export pipelines", "INTERNAL_ERROR", nil)
		return
	}

	// Optional: filter by comma-separated IDs
	idsParam := r.URL.Query().Get("ids")
	if idsParam != "" {
		idSet := make(map[string]bool)
		for _, id := range splitAndTrim(idsParam) {
			idSet[id] = true
		}
		var filtered []PipelineDefinition
		for _, p := range pipelines {
			if idSet[p.ID] {
				filtered = append(filtered, p)
			}
		}
		pipelines = filtered
	}

	exported := make([]ExportedPipeline, 0, len(pipelines))
	for _, p := range pipelines {
		exported = append(exported, ExportedPipeline{
			Name:           p.Name,
			Description:    p.Description,
			ExecutionMode:  p.ExecutionMode,
			CronExpression: p.CronExpression,
			Status:         p.Status,
			Components:     p.Components,
			Connections:    p.Connections,
		})
	}

	resp := ExportPipelinesResponse{
		Version:    "1.0",
		ExportedAt: time.Now().Format(time.RFC3339),
		Pipelines:  exported,
		Total:      len(exported),
	}

	log.Info().Int("count", len(exported)).Msg("Pipelines exported")
	respondWithJSON(w, http.StatusOK, resp)
}

// ImportPipelines handles POST /api/v1/pipelines/import
// Imports pipelines from the portable JSON format.
// If overwrite_by_name is true, existing pipelines with the same name are updated.
func (h *PipelineHandlers) ImportPipelines(w http.ResponseWriter, r *http.Request) {
	var req ImportPipelinesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload", "INVALID_REQUEST", nil)
		return
	}

	if len(req.Pipelines) == 0 {
		respondWithError(w, http.StatusBadRequest, "No pipelines to import", "INVALID_REQUEST", nil)
		return
	}

	// Build name→ID lookup for overwrite mode
	var nameToID map[string]string
	if req.OverwriteByName {
		existing, err := h.engine.GetRepository().List()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to list existing pipelines", "INTERNAL_ERROR", nil)
			return
		}
		nameToID = make(map[string]string, len(existing))
		for _, p := range existing {
			nameToID[p.Name] = p.ID
		}
	}

	imported, skipped, overwritten := 0, 0, 0
	var errors []string

	for i, ep := range req.Pipelines {
		def := PipelineDefinition{
			Name:           ep.Name,
			Description:    ep.Description,
			ExecutionMode:  ep.ExecutionMode,
			CronExpression: ep.CronExpression,
			Status:         ep.Status,
			Components:     ep.Components,
			Connections:    ep.Connections,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}

		// Check for overwrite
		if req.OverwriteByName {
			if existingID, exists := nameToID[ep.Name]; exists {
				def.ID = existingID
				if err := h.engine.GetRepository().Update(def); err != nil {
					errors = append(errors, fmt.Sprintf("pipeline[%d] %q: overwrite failed: %s", i, ep.Name, err.Error()))
					skipped++
				} else {
					overwritten++
				}
				continue
			}
		}

		// Create new
		def.ID = uuid.New().String()
		if err := h.engine.GetRepository().Create(def); err != nil {
			errors = append(errors, fmt.Sprintf("pipeline[%d] %q: %s", i, ep.Name, err.Error()))
			skipped++
		} else {
			imported++
		}
	}

	msg := fmt.Sprintf("Import complete: %d imported, %d overwritten, %d skipped", imported, overwritten, skipped)
	log.Info().Int("imported", imported).Int("overwritten", overwritten).Int("skipped", skipped).Msg("Pipelines imported")

	respondWithJSON(w, http.StatusOK, ImportPipelinesResponse{
		Imported:    imported,
		Skipped:     skipped,
		Overwritten: overwritten,
		Errors:      errors,
		Message:     msg,
	})
}

// splitAndTrim splits a comma-separated string and trims whitespace
func splitAndTrim(s string) []string {
	parts := make([]string, 0)
	for _, p := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

// GetPipelineLogs returns log entries for a pipeline, queryable by the React frontend.
// Supports query params: page, page_size, execution_id, component_id
func (h *PipelineHandlers) GetPipelineLogs(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if pipelineID == "" {
		respondWithError(w, http.StatusBadRequest, "Pipeline ID is required", "INVALID_REQUEST", nil)
		return
	}

	page := 1
	pageSize := 50

	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if pageSizeStr := r.URL.Query().Get("page_size"); pageSizeStr != "" {
		if ps, err := strconv.Atoi(pageSizeStr); err == nil && ps > 0 && ps <= 500 {
			pageSize = ps
		}
	}

	offset := (page - 1) * pageSize
	store := h.engine.GetLogStore()
	if store == nil {
		respondWithError(w, http.StatusServiceUnavailable, "Log store not available", "SERVICE_UNAVAILABLE", nil)
		return
	}

	// Support filtering by execution_id or component_id
	var entries []PipelineLogEntry
	var total int
	var err error

	if execID := r.URL.Query().Get("execution_id"); execID != "" {
		entries, total, err = store.GetLogsByExecution(r.Context(), execID, pageSize, offset)
	} else if compID := r.URL.Query().Get("component_id"); compID != "" {
		entries, total, err = store.GetLogsByComponent(r.Context(), compID, pageSize, offset)
	} else {
		entries, total, err = store.GetLogs(r.Context(), pipelineID, pageSize, offset)
	}

	if err != nil {
		log.Error().Err(err).Str("pipeline_id", pipelineID).Msg("Failed to get pipeline logs")
		respondWithError(w, http.StatusInternalServerError, "Failed to get pipeline logs", "INTERNAL_ERROR", nil)
		return
	}

	respondWithJSON(w, http.StatusOK, GetPipelineLogsResponse{
		Logs:     entries,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}
