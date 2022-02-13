package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/configuration"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/models/version"
	log "github.com/sirupsen/logrus"
	"time"
)

var sqsConfiguration = configuration.SQS{
	PollDuration:   2,
	MessageTimeout: 0,
	Queues: []configuration.Observer{{
		Tag:          "",
		QueueName:    queueName,
		PollDuration: 2,
	}, {
		Tag:          "",
		QueueName:    queue2Name,
		PollDuration: 2,
	},
	},
}

func (s *sqsTestSuite) TestObserversFromConfig() {
	log.SetLevel(log.TraceLevel)
	var (
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
		err         error
	)

	// Launch observers from configuration
	err = LaunchObservers(ctx, stack.sess, sqsConfiguration)
	s.Assert().NoError(err)

	var (
		currentTime    = time.Now()
		exampleMessage = messages.ApiMessage{
			MessageId:       "uuid3",
			MessageType:     messages.Heartbeat,
			Timestamp:       &currentTime,
			ProtocolVersion: version.ProtocolVersion16,
			Data:            "exampleData",
		}

		exampleMessage2 = messages.ApiMessage{
			MessageId:       "uuid4",
			MessageType:     messages.AuthTag,
			Timestamp:       &currentTime,
			ProtocolVersion: version.ProtocolVersion16,
			Data:            "exampleData2",
		}
	)

	StartDefaultPublisher(ctx, stack.sess)

	var (
		// Get observer channel
		observerChan = manager.GetDefaultObserver().GetConsumerChannel()
		// Get the publisher
		pb           = manager.GetDefaultPublisher()
		producerChan = pb.GetProducerChannel()
	)

	// Send a message to the queue
	go func() {
		time.Sleep(3 * time.Second)
		producerChan <- PublisherMessage{
			QueueName: queueName,
			Message:   exampleMessage,
		}

		time.Sleep(3 * time.Second)
		producerChan <- PublisherMessage{
			QueueName: queue2Name,
			Message:   exampleMessage2,
		}
	}()

Loop:
	for {
		select {
		case <-ctx.Done():
			cancel()
			break Loop
		case msg := <-observerChan:
			// The message should be received
			log.Infof("Received message: %v", msg)
			switch msg.MessageType {
			case messages.Heartbeat:
				s.Assert().Equal(exampleMessage.MessageId, msg.MessageId)
				s.Assert().Equal(exampleMessage.Data, msg.Data)
				s.Assert().Equal(exampleMessage.MessageType, msg.MessageType)
				break
			case messages.AuthTag:
				s.Assert().Equal(exampleMessage2.MessageId, msg.MessageId)
				s.Assert().Equal(exampleMessage2.Data, msg.Data)
				s.Assert().Equal(exampleMessage2.MessageType, msg.MessageType)
				cancel()
				break
			default:
				s.FailNow("Not supported message")
				cancel()
			}
			break
		}
	}
}
