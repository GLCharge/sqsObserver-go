package sqsObserver_go

import (
	"context"
	"fmt"
	"github.com/GLCharge/sqsObserver-go/models/configuration"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"
)

// StartDefaultPublisher starts listening to publisher channel in a goroutine. It is non-blocking.
func StartDefaultPublisher(ctx context.Context, session *session.Session) {
	publisher := NewSqsPublisher(session)
	manager := GetObserverManager()
	manager.SetDefaultPublisher(publisher)

	manager.PublisherListen(ctx)
}

// LaunchObservers creates observers based on the provided configuration.
// It launches each observer in a new goroutine. If any of the observers fail to configure, returns an error.
func LaunchObservers(ctx context.Context, session *session.Session, sqsConfiguration configuration.SQS) error {
	log.Debugf("Creating SQS observers from configuration")

	manager := GetObserverManager()

	// Create a default observer
	observerChannel := make(chan messages.ApiMessage, len(sqsConfiguration.Queues)*3)
	defaultObserver := NewMultipleQueueObserverWithChannel(session, observerChannel)
	defaultObserver.SetPollDuration(sqsConfiguration.PollDuration)
	manager.SetDefaultObserver(defaultObserver)

	for _, queue := range sqsConfiguration.Queues {
		var queueName *string

		qOut, err := sqs.GetQueueURLFromSession(session, queue.QueueName)
		if err != nil || qOut.QueueUrl == nil {
			return fmt.Errorf("cannot get URL for queue %s: %v", queue.QueueName, err)
		}

		queueName = qOut.QueueUrl

		// If the tag is empty, add the queue to the default observer
		if queue.Tag == "" {
			defaultObserver.AddQueuesToObserve(*queueName)
			continue
		}

		if manager.HasObserverWithTag(queue.Tag) {
			log.Tracef("Observer with the tag %s already exists, adding queue to the observer: %v", queue.Tag, *queueName)

			// Add the queue to the observer with tag
			mObserver := manager.GetMultipleObserver(queue.Tag)
			if mObserver != nil {
				mObserver.AddQueuesToObserve(*queueName)
			}

		} else {
			// Create a new multiple observer with the queue name
			newObserver := NewMultipleQueueObserverWithChannel(session, observerChannel)
			newObserver.SetPollDuration(sqsConfiguration.PollDuration)
			newObserver.SetTimeout(1)
			newObserver.AddQueuesToObserve(*queueName)
			manager.AddObserver(queue.Tag, newObserver)
		}
	}

	manager.StartObservers(ctx)
	return nil
}
