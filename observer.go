package sqsObserver_go

import (
	"context"
	"encoding/json"
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
		Start(ctx context.Context)
		GetConsumerChannel() <-chan messages.ApiMessage
	}

	SingleObserver interface {
		Observer
	}

	MultipleObserver interface {
		Observer
		AddQueuesToObserve(queueNames ...string)
		SetPollDuration(pollDuration int64)
		SetTimeout(timeout int64)
	}

	// SingleSQSObserver is a concrete implementation of QueueObserver.
	SingleSQSObserver struct {
		consumerChan     chan messages.ApiMessage
		queueListenUrl   string
		pollDuration     *int64
		receivedMessages sync.Map
		svc              *awsSqs.SQS
	}
)

// GetConsumerChannel returns the QueueMessage channel
func (b *SingleSQSObserver) GetConsumerChannel() <-chan messages.ApiMessage {
	return b.consumerChan
}

// Start starts listening to the SQS for incoming messages as well as listening to the channel.
func (b *SingleSQSObserver) Start(ctx context.Context) {
	var (
		timeout = int64(1)
	)

Loop:
	for {
		select {
		// Listen to context
		case <-ctx.Done():
			close(b.consumerChan)
			log.Debug("Stopping queue observer..")
			break Loop
		default:
			// Poll the queue for messages
			log.Tracef("Polling queue: %s", b.queueListenUrl)
			qMessages, err := sqs.GetMessages(b.svc, b.queueListenUrl, &timeout, b.pollDuration)
			if err != nil {
				log.Errorf("Cannot retrieve a message from SQS: %v", err)
				continue
			}

			if qMessages != nil {
				for _, message := range qMessages.Messages {
					b.sendMessageToChannel(message)
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
}

// sendMessageToChannel sends a message received from SQS to the channel.
func (b *SingleSQSObserver) sendMessageToChannel(message *awsSqs.Message) {
	var (
		sqsMessage  messages.ApiMessage
		messageBody = message.Body
	)

	if messageBody != nil {
		err := json.Unmarshal([]byte(*messageBody), &sqsMessage)
		if err != nil {
			log.Errorf("Error unmarshalling message: %v", err)
			return
		}

		defer func(svc *awsSqs.SQS, queueURL *string, messageHandle *string) {
			err := sqs.DeleteMessage(svc, queueURL, messageHandle)
			if err != nil {
				log.Warnf("Couldn't delete message: %v", err)
			}
		}(b.svc, &b.queueListenUrl, message.ReceiptHandle)

		log.Tracef("Sending message %v to channel", sqsMessage)
		b.consumerChan <- sqsMessage
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

	return &SingleSQSObserver{
		consumerChan:   make(chan messages.ApiMessage, 10),
		pollDuration:   &queueConfig.PollDuration,
		queueListenUrl: *url.QueueUrl,
		svc:            svc,
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

	return &SingleSQSObserver{
		consumerChan:   make(chan messages.ApiMessage, 10),
		pollDuration:   &pollDuration,
		queueListenUrl: *url.QueueUrl,
		svc:            svc,
	}
}
