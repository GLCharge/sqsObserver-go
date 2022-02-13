package sqsObserver_go

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GLCharge/sqsObserver-go/models/configuration"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type (
	//Observer is an observer/consumer interface for the service API.
	Observer interface {
		Start(ctx context.Context) error
		GetConsumerChannel() <-chan messages.ApiMessage
		SetChannel(channel chan messages.ApiMessage)
		SetLogger(logger *log.Logger)
	}

	SingleObserver interface {
		Observer
	}

	MultipleObserver interface {
		Observer
		AddQueuesToObserve(queues ...Queue) error
		SetDefaultPollDuration(pollDuration int64)
		SetDefaultTimeout(timeout int64)
	}

	Queue struct {
		QueueName         string
		QueueUrl          *string
		PollDuration      *int64
		VisibilityTimeout *int64
	}

	// SingleSQSObserver is a concrete implementation of QueueObserver.
	SingleSQSObserver struct {
		consumerChan     chan messages.ApiMessage
		queue            Queue
		receivedMessages sync.Map
		svc              *awsSqs.SQS
		logger           *log.Logger
	}
)

// GetConsumerChannel returns the QueueMessage channel
func (o *SingleSQSObserver) GetConsumerChannel() <-chan messages.ApiMessage {
	return o.consumerChan
}

// SetChannel returns the QueueMessage channel
func (o *SingleSQSObserver) SetChannel(channel chan messages.ApiMessage) {
	if channel != nil {
		o.consumerChan = channel
	}
}

// SetLogger sets the logger for this instance
func (o *SingleSQSObserver) SetLogger(logger *log.Logger) {
	if logger != nil {
		o.logger = logger
	}
}

// Start starts listening to the SQS for incoming messages as well as listening to the channel.
func (o *SingleSQSObserver) Start(ctx context.Context) error {
	if o.queue.QueueUrl == nil {
		return errors.New("queue url must not be nil")
	}

	var (
		queueUrl          = o.queue.QueueUrl
		visibilityTimeout = o.queue.VisibilityTimeout
		pollDuration      = o.queue.PollDuration
	)

Loop:
	for {
		select {
		// Listen to context
		case <-ctx.Done():
			close(o.consumerChan)
			o.logger.Debug("Stopping queue observer..")
			break Loop
		default:
			// Poll the queue for messages
			o.logger.Tracef("Polling queue: %v", queueUrl)

			qMessages, err := sqs.GetMessages(o.svc, *queueUrl, visibilityTimeout, pollDuration)
			if err != nil {
				o.logger.WithError(err).Errorf("Cannot retrieve a message from SQS")
				return err
			}

			if qMessages != nil {
				for _, message := range qMessages.Messages {
					o.sendMessageToChannel(message)
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

	return nil
}

// sendMessageToChannel sends a message received from SQS to the channel.
func (o *SingleSQSObserver) sendMessageToChannel(message *awsSqs.Message) {
	var (
		sqsMessage  messages.ApiMessage
		messageBody = message.Body
	)

	if messageBody != nil {
		err := json.Unmarshal([]byte(*messageBody), &sqsMessage)
		if err != nil {
			o.logger.Errorf("Error unmarshalling message: %v", err)
			return
		}

		// Delete the message from queue
		defer func(svc *awsSqs.SQS, queueURL *string, messageHandle *string) {
			err := sqs.DeleteMessage(svc, queueURL, messageHandle)
			if err != nil {
				o.logger.Warnf("Couldn't delete message: %v", err)
			}
		}(o.svc, o.queue.QueueUrl, message.ReceiptHandle)

		// Send the message to the channel
		o.logger.Tracef("Sending message %v to channel", sqsMessage)
		o.consumerChan <- sqsMessage
	}
}

// NewSqsSingleObserverFromConfiguration creates a new SQS SingleObserver from configuration
func NewSqsSingleObserverFromConfiguration(session *session.Session, queueConfig configuration.Observer) SingleObserver {
	log.Debug("Creating a new SQS observer from configuration")

	// Create an SQS service client
	mutex := sync.Mutex{}
	mutex.Lock()
	svc := awsSqs.New(session)
	mutex.Unlock()

	// Get the SQS url
	url, err := sqs.GetQueueURL(svc, queueConfig.Tag)
	if err != nil {
		return nil
	}

	visibilityTimeout := int64(0)

	return &SingleSQSObserver{
		consumerChan: make(chan messages.ApiMessage, 10),
		queue: Queue{
			QueueName:         queueConfig.QueueName,
			QueueUrl:          url.QueueUrl,
			PollDuration:      &queueConfig.PollDuration,
			VisibilityTimeout: &visibilityTimeout,
		},
		svc:    svc,
		logger: log.StandardLogger(),
	}
}

// NewSqsSingleObserver creates a new SQS SingleObserver
func NewSqsSingleObserver(session *session.Session, queueName string, pollDuration int64) SingleObserver {
	log.Debugf("Creating a new SQS observer for queue: %s", queueName)

	// Create an SQS service client
	mutex := sync.Mutex{}
	mutex.Lock()
	svc := awsSqs.New(session)
	mutex.Unlock()

	// Get the SQS url
	url, err := sqs.GetQueueURL(svc, queueName)
	if err != nil {
		return nil
	}

	visibilityTimeout := int64(0)

	return &SingleSQSObserver{
		consumerChan: make(chan messages.ApiMessage, 10),
		queue: Queue{
			QueueName:         queueName,
			QueueUrl:          url.QueueUrl,
			PollDuration:      &pollDuration,
			VisibilityTimeout: &visibilityTimeout,
		},
		svc:    svc,
		logger: log.StandardLogger(),
	}
}
