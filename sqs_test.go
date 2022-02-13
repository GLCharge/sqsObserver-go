package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/models/version"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

var (
	queueName  = "HeartbeatQueue"
	queue2Name = "ExampleQueue"
	queue3Name = "Example1Queue"
	stack      *Localstack
)

type sqsTestSuite struct {
	suite.Suite
}

func (s *sqsTestSuite) SetupTest() {
	var svc = awsSqs.New(stack.sess)
	// Create queues
	err := createQueues(svc, queueName, queue2Name, queue3Name)
	s.Assert().NoError(err)
}

func createQueues(svc *awsSqs.SQS, queues ...string) error {
	for _, queue := range queues {
		_, err := sqs.CreateQueue(svc, &queue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *sqsTestSuite) TestPublisherAndSingleObserver() {
	var (
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
		// Create a new observer
		testObserver = NewSqsSingleObserver(stack.sess, queue3Name, 3)
		// Create a new publisher
		pb = NewSqsPublisher(stack.sess)
	)

	// Observe the queue
	go func() {
		err := testObserver.Start(ctx)
		s.Assert().NoError(err)
	}()

	// Publisher listens to the channel
	go pb.Listen(ctx)

	var (
		currentTime    = time.Now()
		exampleMessage = messages.ApiMessage{
			MessageId:       "uuid",
			MessageType:     messages.SetChargingProfile,
			Timestamp:       &currentTime,
			ProtocolVersion: version.ProtocolVersion16,
			Data:            "exampleData",
		}
		ch         = pb.GetProducerChannel()
		consumerCh = testObserver.GetConsumerChannel()
	)

	// Send a message to the queue
	go func() {
		time.Sleep(3 * time.Second)
		ch <- PublisherMessage{
			QueueName: queue3Name,
			Message:   exampleMessage,
		}
	}()

Loop:
	for {
		select {
		case <-ctx.Done():
			cancel()
			break Loop
		case msg := <-consumerCh:
			// The message should be received
			log.Infof("Received message: %v", msg)
			s.Assert().Equal(exampleMessage.MessageId, msg.MessageId)
			s.Assert().Equal(exampleMessage.Data, msg.Data)
			s.Assert().Equal(exampleMessage.MessageType, msg.MessageType)

			cancel()
			break
		}
	}
}

func (s *sqsTestSuite) TestMultipleObserver() {
	var (
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
		svc         = awsSqs.New(stack.sess)
		err         error
		// Create a new observer
		testObserver = NewMultipleQueueObserver(stack.sess)
		// Create a new publisher
		pb = NewSqsPublisher(stack.sess)
	)

	q1 := CreateQueue(svc, queueName, aws.Int64(2), nil)
	q2 := CreateQueue(svc, queue2Name, aws.Int64(2), nil)
	s.Require().NotNil(q1)
	s.Require().NotNil(q2)

	err = testObserver.AddQueuesToObserve(*q1, *q2)
	s.Require().NoError(err)

	go func() {
		err := testObserver.Start(ctx)
		s.Assert().NoError(err)
	}()

	var (
		currentTime    = time.Now()
		exampleMessage = messages.ApiMessage{
			MessageId:       "uuid1",
			MessageType:     messages.Heartbeat,
			Timestamp:       &currentTime,
			ProtocolVersion: version.ProtocolVersion16,
			Data:            "exampleData",
		}
		exampleMessage2 = messages.ApiMessage{
			MessageId:       "uuid2",
			MessageType:     messages.AuthTag,
			Timestamp:       &currentTime,
			ProtocolVersion: version.ProtocolVersion16,
			Data:            "exampleData2",
		}
	)

	go pb.Listen(ctx)

	// Send a message to the queue
	go func() {
		time.Sleep(3 * time.Second)
		pb.GetProducerChannel() <- PublisherMessage{
			QueueName: queueName,
			Message:   exampleMessage,
		}

		time.Sleep(3 * time.Second)
		pb.GetProducerChannel() <- PublisherMessage{
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
		case msg := <-testObserver.GetConsumerChannel():
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
				s.FailNow("Not a supported message")
				cancel()
			}
			break
		}
	}
}

func TestSQS(t *testing.T) {
	assert := assert2.New(t)

	// Create a new AWS stack
	localstack, err := NewLocalstack(LocalstackConfig{
		Region:                     "eu-central-1",
		Services:                   []string{"sqs"},
		ContainerExpirationSeconds: 100,
		BackoffDuration:            100,
	})
	assert.NoError(err)

	time.Sleep(5 * time.Second)

	stack = localstack

	suite.Run(t, new(sqsTestSuite))
}
