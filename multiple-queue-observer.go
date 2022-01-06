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
	"time"
)

type (
	//MultipleQueueObserver is a concrete implementation of QueueObserver.
	MultipleQueueObserver struct {
		channel             chan messages.ApiMessage
		defaultPollDuration int64
		timeout             int64
		queues              sync.Map
		pollDurations       map[string]int64
		svc                 *awsSqs.SQS
	}
)

// NewMultipleQueueObserver creates a new observer with multiple queues.
func NewMultipleQueueObserver(session *session.Session) *MultipleQueueObserver {
	// Create an SQS service client
	mutex := sync.Mutex{}
	mutex.Lock()
	svc := awsSqs.New(session)
	mutex.Unlock()

	return &MultipleQueueObserver{
		channel:             make(chan messages.ApiMessage, 20),
		queues:              sync.Map{},
		defaultPollDuration: 2,
		timeout:             1,
		pollDurations:       make(map[string]int64),
		svc:                 svc,
	}
}

// NewMultipleQueueObserverWithChannel creates a new observer with multiple queues with shared channel.
func NewMultipleQueueObserverWithChannel(session *session.Session, messageChan chan messages.ApiMessage) *MultipleQueueObserver {
	if messageChan == nil {
		return nil
	}

	// Create an SQS service client
	mutex := sync.Mutex{}
	mutex.Lock()
	svc := awsSqs.New(session)
	mutex.Unlock()

	return &MultipleQueueObserver{
		channel:             messageChan,
		queues:              sync.Map{},
		defaultPollDuration: 2,
		timeout:             1,
		pollDurations:       make(map[string]int64),
		svc:                 svc,
	}
}

//GetConsumerChannel returns the ApiMessage channel
func (mqo *MultipleQueueObserver) GetConsumerChannel() <-chan messages.ApiMessage {
	return mqo.channel
}

//Start starts listening to the SQS for incoming messages as well as listening to the channel.
func (mqo *MultipleQueueObserver) Start(ctx context.Context) {
	var (
		i         = 0
		waitTime  = mqo.defaultPollDuration
		queueUrls []string
	)

	mqo.queues.Range(func(key, value interface{}) bool {
		queueUrls = append(queueUrls, value.(string))
		return true
	})

	log.Debugf("Started the observer with queues: %v", queueUrls)

	if len(queueUrls) == 0 {
		log.Debug("Observer listening to no queues, returning..")
		return
	}

Loop:
	for {
		select {
		// Listen to context
		case <-ctx.Done():
			log.Info("Stopping the multiple queue observer..")
			close(mqo.channel)
			break Loop
		default:
			// Reset the queue index
			if i > len(queueUrls)-1 {
				i = 0
			}

			if len(queueUrls) > 0 {
				log.Tracef("Polling queue: %s", queueUrls[i])

				queueUrl := queueUrls[i]
				qMessages, err := sqs.GetMessages(mqo.svc, queueUrl, &mqo.timeout, &waitTime)
				if err != nil {
					log.Errorf("Error getting messages: %v", err)
					continue
				}

				if qMessages != nil {
					for _, message := range qMessages.Messages {
						mqo.sendMessageToChannel(queueUrl, message)
					}
				}
				i++
			}

			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (mqo *MultipleQueueObserver) AddQueuesToObserve(queueNames ...string) {
	log.WithField("queues", queueNames).Debug("Adding queues to observe")

	if queueNames != nil {
		// Get urls for the queues
		for _, queueName := range queueNames {
			url, err := sqs.GetQueueURL(mqo.svc, queueName)
			if err != nil {
				continue
			}

			if url != nil {
				mqo.queues.Store(queueName, *url.QueueUrl)
			}
		}
	}
}

func (mqo *MultipleQueueObserver) SetPollDuration(pollDuration int64) {
	mqo.defaultPollDuration = pollDuration
}

func (mqo *MultipleQueueObserver) SetTimeout(timeout int64) {
	mqo.timeout = timeout
}

func (mqo *MultipleQueueObserver) sendMessageToChannel(queueUrl string, message *awsSqs.Message) {
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

		log.Debugf("Sending message to channel: %v", *messageBody)
		mqo.channel <- sqsMessage

		err = sqs.DeleteMessage(mqo.svc, &queueUrl, message.ReceiptHandle)
		if err != nil {
			log.Warnf("Couldn't delete the message from queue: %v", err)
		}
	}
}
