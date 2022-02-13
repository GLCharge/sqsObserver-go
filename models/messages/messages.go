package messages

import (
	"encoding/json"
	"errors"
	"github.com/GLCharge/sqsObserver-go/models/version"
	"time"
)

// MessageType constants
const (
	BootNotification            = MessageType("BootNotification")
	DisconnectedNotification    = MessageType("DisconnectedNotification")
	StatusNotification          = MessageType("StatusNotification")
	AuthTag                     = MessageType("TagAuthentication")
	Heartbeat                   = MessageType("Heartbeat")
	StartTransaction            = MessageType("StartTransaction")
	StopTransaction             = MessageType("StopTransaction")
	RemoteStartTransaction      = MessageType("RemoteStartTransaction")
	RemoteStopTransaction       = MessageType("RemoteStopTransaction")
	MeterValue                  = MessageType("MeterValue")
	UnlockConnector             = MessageType("UnlockConnector")
	Reset                       = MessageType("Reset")
	ChangeConfiguration         = MessageType("ChangeConfiguration")
	GetConfiguration            = MessageType("GetConfiguration")
	ChangeAvailability          = MessageType("ChangeAvailability")
	DataTransfer                = MessageType("DataTransfer")
	SetChargingProfile          = MessageType("SetChargingProfile")
	ClearChargingProfile        = MessageType("ClearChargingProfile")
	TriggerMessage              = MessageType("TriggerMessage")
	GetChargePointStatus        = MessageType("GetChargePointStatus")
	GetStructureFromEnergyMeter = MessageType("GetStructureFromEnergyMeter")

	StatusProcessed = "Processed"
	StatusError     = "Error"
)

var (
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	ErrCpIdInvalid        = errors.New("cp cannot be an empty string")
)

type (
	MessageType string

	//ApiMessage represents a generic message used for communication with the service.
	ApiMessage struct {
		MessageId       string                  `json:"messageId" validate:"required"`
		MessageType     MessageType             `json:"messageType" validate:"required,isSupportedMessageType"`
		TraceId         string                  `json:"traceId,omitempty"`
		SpanId          string                  `json:"spanId,omitempty"`
		ParentSpanId    string                  `json:"parentSpanId,omitempty"`
		Timestamp       *time.Time              `json:"timestamp,omitempty"`
		ProtocolVersion version.ProtocolVersion `json:"protocolVersion" validate:"isSupportedProtocol"`
		Status          string                  `json:"status,omitempty"`
		Errors          []string                `json:"errors,omitempty"`
		Data            interface{}             `json:"data,omitempty"`
	}
)

func (mt MessageType) String() string {
	return string(mt)
}

func (qm *ApiMessage) String() string {
	out, err := json.Marshal(&qm)
	if err != nil {
		return ""
	}

	return string(out)
}
