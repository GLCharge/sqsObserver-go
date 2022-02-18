package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/configuration"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

// StartDefaultPublisher starts listening to publisher channel in a goroutine. It is non-blocking.
func StartDefaultPublisher(ctx context.Context, session *session.Session) {
	publisher := NewSqsPublisher(session)
	manager := GetObserverManager()
	manager.SetDefaultPublisher(publisher)
	manager.PublisherListen(ctx)
}

// CreateQueue gets the queue URL from name and creates a Queue.
func CreateQueue(client *awsSqs.SQS, queueName string, pollDuration, timeoutDuration *int64) *Queue {
	url, err := sqs.GetQueueURL(client, queueName)
	if err != nil {
		return nil
	}

	return &Queue{
		QueueName:         queueName,
		QueueUrl:          url.QueueUrl,
		PollDuration:      pollDuration,
		VisibilityTimeout: timeoutDuration,
	}
}

// LaunchObservers Create observers based on the provided configuration.
// It launches each observer in a new goroutine with a common channel. Returns an error if any of the observers
// is unable to configure properly.
func LaunchObservers(ctx context.Context, session *session.Session, sqsConfiguration configuration.SQS) error {
	log.Debugf("Creating SQS observers from configuration")

	var (
		manager = GetObserverManager()

		// Create a default observer
		observerChannel = make(chan messages.ApiMessage, len(sqsConfiguration.Queues)*3)
		defaultObserver = NewMultipleQueueObserverWithChannel(session, observerChannel)
		client          = awsSqs.New(session)
	)

	defaultObserver.SetDefaultPollDuration(sqsConfiguration.PollDuration)
	manager.SetDefaultObserver(defaultObserver)

	for _, queue := range sqsConfiguration.Queues {
		var (
			queueName = queue.QueueName
			err       error
		)

		// Create Queue struct from provided configuration
		q := CreateQueue(client, queue.QueueName, &queue.PollDuration, &queue.MessageTimeout)
		if q == nil {
			return ErrQueueDoesntExist
		}

		// If the tag is empty, add the queue to the default observer
		if queue.Tag == "" {
			err = defaultObserver.AddQueuesToObserve(*q)
			if err != nil {
				return err
			}

			continue
		}

		// If tag is already present, add the queue to existing observer
		if manager.HasObserverWithTag(queue.Tag) {
			log.Tracef("Observer with the tag %s already exists, adding queue to the observer: %v", queue.Tag, queueName)

			// Add the queue to the observer with tag
			mObserver := manager.GetMultipleObserver(queue.Tag)
			if mObserver != nil {
				err = mObserver.AddQueuesToObserve(*q)
				if err != nil {
					return err
				}
			}

		} else {
			// Create a new multiple observer with the queue name
			newObserver := NewMultipleQueueObserverWithChannel(session, observerChannel)
			newObserver.SetDefaultPollDuration(sqsConfiguration.PollDuration)
			newObserver.SetDefaultTimeout(1)
			// Add queue to observer
			err = newObserver.AddQueuesToObserve(*q)
			if err != nil {
				return err
			}

			manager.AddObserver(queue.Tag, newObserver)
		}
	}

	manager.StartObservers(ctx)
	return nil
}
