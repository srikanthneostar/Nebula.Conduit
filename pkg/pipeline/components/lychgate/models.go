package lychgate

import "time"

// Operation represents the type of request operation.
type Operation string

const (
	OperationCreate Operation = "CREATE"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
)

// RequestStatus represents the status of a system request.
type RequestStatus string

const (
	RequestStatusPending   RequestStatus = "Pending"
	RequestStatusCompleted RequestStatus = "Completed"
	RequestStatusFailure   RequestStatus = "Failure"
	RequestStatusCancelled RequestStatus = "Cancelled"
	RequestStatusApplied   RequestStatus = "Applied"
)

// ApplicationSystems represents an application system involved in the request.
type ApplicationSystems struct {
	ID   int    `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// AuditTrail represents a single audit trail entry.
type AuditTrail struct {
	ID        int       `json:"id,omitempty"`
	Action    string    `json:"action,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Details   string    `json:"details,omitempty"`
}

// EntityProperties represents properties associated with an entity.
type EntityProperties struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Entity represents a single entity in the request.
type Entity struct {
	ID   int    `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// SystemRequest mirrors the Python SystemRequest class.
type SystemRequest struct {
	ID               *int                `json:"id,omitempty"`
	IsProcessed      *bool               `json:"isprocessed,omitempty"`
	Requestor        *string             `json:"requestor,omitempty"`
	RequestType      *string             `json:"requesttype,omitempty"`
	RequestStatus    RequestStatus       `json:"requeststatus,omitempty"`
	RequestJSON      *string             `json:"requestjson,omitempty"`
	RequestedDate    *time.Time          `json:"requesteddate,omitempty"`
	RequestComments  *string             `json:"requestcomments,omitempty"`
	MessageID        *string             `json:"messageid,omitempty"`
	EntityID         *int                `json:"entityid,omitempty"`
	EntityType       *string             `json:"entitytype,omitempty"`
	CreateOnDate     *time.Time          `json:"createondate,omitempty"`
	ModifyOnDate     *time.Time          `json:"modifyondate,omitempty"`
	ObjectID         *int                `json:"objectid,omitempty"`
	RequestActions   *string             `json:"requestactions,omitempty"`
	RequestOperation Operation           `json:"requestoperation,omitempty"`
	OriginalRequest  *bool               `json:"originalrequest,omitempty"`
	ForceBroadcast   *bool               `json:"forcebroadcast,omitempty"`
	UpdateCount      *int                `json:"updatecount,omitempty"`
	System           *ApplicationSystems `json:"system,omitempty"`
	OriginSystem     *ApplicationSystems `json:"originsystem,omitempty"`
	AuditTrails      []AuditTrail        `json:"auditTrails,omitempty"`
}

// RequestPayload mirrors the Python RequestPayload class.
// It is the top-level structure sent through the Lychgate messaging system.
type RequestPayload struct {
	SystemRequests SystemRequest      `json:"systemRequests"`
	Properties     []EntityProperties `json:"properties,omitempty"`
	Entities       []Entity           `json:"entities,omitempty"`
	SchemaClass    *string            `json:"schemaClass,omitempty"`
}
