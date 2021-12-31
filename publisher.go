package sqsObserver_go

import (
	"context"
	"encoding/json"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"sync"
)

type (
	//Publisher is publisher/producer interface for the service API.
	Publisher interface {
		Send(queue string, message messages.ApiMessage) error
		Listen(ctx context.Context)
		GetProducerChannel() chan<- PublisherMessage
	}

	SqsPublisher struct {
		once         sync.Once
		producerChan chan PublisherMessage
		queueUrls    sync.Map
		svc          *awsSqs.SQS
	}

	PublisherMessage struct {
		QueueName string
		Message   messages.ApiMessage
	}
)

// NewSqsPublisher creates a new Queue Publisher
func NewSqsPublisher(session *session.Session) Publisher {
	log.Debug("Creating a new SQS publisher..")

	// Create an SQS service client
	svc := awsSqs.New(session)

	return &SqsPublisher{
		producerChan: make(chan PublisherMessage, 20),
		queueUrls:    sync.Map{},
		once:         sync.Once{},
		svc:          svc,
	}
}

// GetProducerChannel returns the QueueMessage channel
func (b *SqsPublisher) GetProducerChannel() chan<- PublisherMessage {
	return b.producerChan
}

// getQueueUrl check the publisher cache for queue URL based on the message type or create and cache the url.
func (b *SqsPublisher) getQueueUrl(queueName string) (*string, error) {
	var (
		queueUrl           *string
		cachedUrl, isFound = b.queueUrls.Load(queueName)
	)

	if !isFound {
		// If the url is not cached, get the URL from message type
		out, err := sqs.GetQueueURL(b.svc, queueName)
		if err != nil {
			return nil, err
		}

		queueUrl = out.QueueUrl
		b.queueUrls.Store(queueName, *queueUrl)
	} else {
		url := cachedUrl.(string)
		queueUrl = &url
	}

	return queueUrl, nil
}

// Send a message to SQS.
func (b *SqsPublisher) Send(queue string, message messages.ApiMessage) error {
	log.Tracef("Sending message %v to queue %s", message, queue)

	sMessage, err := json.Marshal(&message)
	if err != nil {
		return err
	}

	url, err := b.getQueueUrl(queue)
	if err != nil {
		return err
	}

	return sqs.SendMsg(b.svc, url, nil, string(sMessage))
}

// Listen to all incoming messages from the channel and send them to the SQS.
func (b *SqsPublisher) Listen(ctx context.Context) {
	log.Debug("Publisher listening to channel")
	b.once.Do(func() {

	Listener:
		for {
			select {
			// Listen to context
			case <-ctx.Done():
				log.Debug("Stopping the publisher..")
				close(b.producerChan)
				break Listener
			// Listen to the channel
			case messageFromChannel := <-b.producerChan:
				log.Tracef("Publisher received a message from channel: %v", messageFromChannel)

				err := b.Send(messageFromChannel.QueueName, messageFromChannel.Message)
				if err != nil {
					log.Errorf("Cannot send a message to SQS: %v", err)
				}
				break
			}
		}
	})
}
