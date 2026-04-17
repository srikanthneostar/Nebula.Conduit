package lychgate

import (
	"time"

	"github.com/google/uuid"
)

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

func NewRequestPayload(systemId int, entityId int,
	requestJson string, schemaClass *string) *RequestPayload {
	msgID := uuid.New().String()
	requestType := "EntityDataChange"
	entiyType := "TOPICTABLE"

	now := time.Now()

	request := SystemRequest{
		ID:              intPtr(0),
		ObjectID:        intPtr(0),
		IsProcessed:     boolPtr(false),
		OriginalRequest: boolPtr(true),
		System: &ApplicationSystems{
			ID: *intPtr(1),
		},
		OriginSystem: &ApplicationSystems{
			ID: *intPtr(systemId),
		},
		RequestType:   &requestType,
		RequestStatus: RequestStatusPending,
		MessageID:     &msgID,
		RequestJSON:   &requestJson,
		EntityType:    &entiyType,
		CreateOnDate:  &now,
	}

	return &RequestPayload{
		SystemRequests: request,
		SchemaClass:    schemaClass,
	}
}
