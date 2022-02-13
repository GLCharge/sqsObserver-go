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
		SetLogger(logger *log.Logger)
	}

	SqsPublisher struct {
		once         sync.Once
		producerChan chan PublisherMessage
		queueUrls    sync.Map
		svc          *awsSqs.SQS
		logger       *log.Logger
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
		logger:       log.StandardLogger(),
	}
}

// GetProducerChannel returns the QueueMessage channel
func (p *SqsPublisher) GetProducerChannel() chan<- PublisherMessage {
	return p.producerChan
}

// SetLogger set a logger for this instance
func (p *SqsPublisher) SetLogger(logger *log.Logger) {
	if logger != nil {
		p.logger = logger
	}
}

// getQueueUrl check the publisher cache for queue URL based on the message type or create and cache the url.
func (p *SqsPublisher) getQueueUrl(queueName string) (*string, error) {
	var (
		queueUrl           *string
		cachedUrl, isFound = p.queueUrls.Load(queueName)
	)

	if !isFound {
		// If the url is not cached, get the URL from message type
		out, err := sqs.GetQueueURL(p.svc, queueName)
		if err != nil {
			return nil, err
		}

		queueUrl = out.QueueUrl
		p.queueUrls.Store(queueName, *queueUrl)
	} else {
		url := cachedUrl.(string)
		queueUrl = &url
	}

	return queueUrl, nil
}

// Send a message to SQS.
func (p *SqsPublisher) Send(queue string, message messages.ApiMessage) error {
	p.logger.Tracef("Sending message %v to queue %s", message, queue)

	sMessage, err := json.Marshal(&message)
	if err != nil {
		return err
	}

	url, err := p.getQueueUrl(queue)
	if err != nil {
		return err
	}

	return sqs.SendMsg(p.svc, url, nil, string(sMessage))
}

// Listen to all incoming messages from the channel and send them to the SQS.
func (p *SqsPublisher) Listen(ctx context.Context) {
	p.logger.Debug("Publisher listening to channel")
	p.once.Do(func() {

	Listener:
		for {
			select {
			// Listen to context
			case <-ctx.Done():
				p.logger.Debug("Stopping the publisher..")
				close(p.producerChan)
				break Listener
			// Listen to the channel
			case messageFromChannel := <-p.producerChan:
				p.logger.Tracef("Publisher received a message from channel: %v", messageFromChannel)

				err := p.Send(messageFromChannel.QueueName, messageFromChannel.Message)
				if err != nil {
					p.logger.Errorf("Cannot send a message to SQS: %v", err)
				}
				break
			}
		}
	})
}
