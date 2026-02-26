package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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
