package sqsObserver_go

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

var (
	ErrNoQueue          = errors.New("no queues provided")
	ErrQueueDoesntExist = errors.New("queue does not exist")
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
		logger              *log.Logger
	}
)

// NewMultipleQueueObserver creates a new observer with multiple queues.
func NewMultipleQueueObserver(session *session.Session) MultipleObserver {
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
		logger:              log.StandardLogger(),
	}
}

// NewMultipleQueueObserverWithChannel creates a new observer with multiple queues with shared channel.
func NewMultipleQueueObserverWithChannel(session *session.Session, messageChan chan messages.ApiMessage) MultipleObserver {
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
		logger:              log.StandardLogger(),
	}
}

//GetConsumerChannel returns the ApiMessage channel
func (mqo *MultipleQueueObserver) GetConsumerChannel() <-chan messages.ApiMessage {
	return mqo.channel
}

// SetChannel returns the QueueMessage channel
func (mqo *MultipleQueueObserver) SetChannel(channel chan messages.ApiMessage) {
	if channel != nil {
		mqo.channel = channel
	}
}

//Start starts listening to the SQS for incoming messages as well as listening to the channel.
func (mqo *MultipleQueueObserver) Start(ctx context.Context) error {
	var (
		i      = 0
		queues []*Queue
	)

	mqo.queues.Range(func(key, value interface{}) bool {
		queues = append(queues, value.(*Queue))
		return true
	})

	mqo.logger.Debugf("Started the observer with queues: %v", queues)

	if len(queues) == 0 {
		mqo.logger.Debug("Observer listening to no queues, returning..")
		return ErrNoQueue
	}

Loop:
	for {
		select {
		// Listen to context
		case <-ctx.Done():
			mqo.logger.Info("Stopping the multiple queue observer..")
			close(mqo.channel)
			break Loop
		default:
			// Reset the queue index
			if i > len(queues)-1 {
				i = 0
			}

			if len(queues) > 0 {
				queue := queues[i]

				mqo.logger.Tracef("Polling queue: %v", queue)

				// Get the messages from SQS
				qMessages, err := sqs.GetMessages(mqo.svc, *queue.QueueUrl, queue.VisibilityTimeout, queue.PollDuration)
				if err != nil {
					mqo.logger.Errorf("Error getting messages: %v", err)
					continue
				}

				if qMessages != nil {
					for _, message := range qMessages.Messages {
						mqo.sendMessageToChannel(*queue.QueueUrl, message)
					}
				}
				i++
			}

			time.Sleep(30 * time.Millisecond)
		}
	}

	return nil
}

func (mqo *MultipleQueueObserver) AddQueuesToObserve(queues ...Queue) error {
	mqo.logger.Debug("Adding queues to observe")

	if queues == nil {
		return ErrNoQueue
	}

	for _, queue := range queues {
		// Check if QueueUrl is not nil
		if queue.QueueUrl != nil {
			mqo.queues.Store(queue.QueueName, &queue)
		} else {
			return ErrQueueDoesntExist
		}

	}

	return nil
}

func (mqo *MultipleQueueObserver) SetDefaultPollDuration(pollDuration int64) {
	mqo.defaultPollDuration = pollDuration
}

func (mqo *MultipleQueueObserver) SetDefaultTimeout(timeout int64) {
	mqo.timeout = timeout
}

func (mqo *MultipleQueueObserver) SetLogger(logger *log.Logger) {
	if logger != nil {
		mqo.logger = logger
	}
}

func (mqo *MultipleQueueObserver) sendMessageToChannel(queueUrl string, message *awsSqs.Message) {
	var (
		sqsMessage  messages.ApiMessage
		messageBody = message.Body
		err         error
	)

	defer func() {
		err = sqs.DeleteMessage(mqo.svc, &queueUrl, message.ReceiptHandle)
		if err != nil {
			mqo.logger.Warnf("Couldn't delete the message from queue: %v", err)
		}
	}()

	if messageBody != nil {
		err = json.Unmarshal([]byte(*messageBody), &sqsMessage)
		if err != nil {
			mqo.logger.Errorf("Error unmarshalling message: %v", err)
			return
		}

		mqo.logger.Debugf("Sending message to channel: %v", *messageBody)
		mqo.channel <- sqsMessage
	}
}
