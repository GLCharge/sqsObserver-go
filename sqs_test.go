package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/models/version"
	"github.com/GLCharge/sqsObserver-go/sqs"
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
	stack      *Localstack
)

type sqsTestSuite struct {
	suite.Suite
}

func (s *sqsTestSuite) SetupTest() {

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	svc := awsSqs.New(stack.sess)

	// Create queues
	err := createQueues(svc, queueName)
	s.Assert().NoError(err)

	// Create a new observer
	testObserver := NewSqsSingleObserver(stack.sess, queueName, 3)
	go testObserver.Start(ctx)

	// Create a new publisher
	pb := NewSqsPublisher(stack.sess)
	go pb.Listen(ctx)

	currentTime := time.Now()
	exampleMessage := messages.ApiMessage{
		MessageId:       "uuid",
		MessageType:     messages.Heartbeat,
		Timestamp:       &currentTime,
		ProtocolVersion: version.ProtocolVersion16,
		Data:            "exampleData",
	}

	// Send a message to the queue
	go func() {
		time.Sleep(1 * time.Second)
		pb.GetProducerChannel() <- PublisherMessage{
			QueueName: queueName,
			Message:   exampleMessage,
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
			s.Assert().Equal(exampleMessage.MessageId, msg.MessageId)
			s.Assert().Equal(exampleMessage.Data, msg.Data)
			s.Assert().Equal(exampleMessage.MessageType, msg.MessageType)

			cancel()
			break
		}
	}
}

func (s *sqsTestSuite) TestMultipleObserver() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	svc := awsSqs.New(stack.sess)

	err := createQueues(svc, queueName, queue2Name)
	s.Assert().NoError(err)

	// Create a new observer
	testObserver := NewMultipleQueueObserver(stack.sess)
	testObserver.AddQueuesToObserve(queueName, queue2Name)
	go testObserver.Start(ctx)

	currentTime := time.Now()
	exampleMessage := messages.ApiMessage{
		MessageId:       "uuid1",
		MessageType:     messages.Heartbeat,
		Timestamp:       &currentTime,
		ProtocolVersion: version.ProtocolVersion16,
		Data:            "exampleData",
	}

	exampleMessage2 := messages.ApiMessage{
		MessageId:       "uuid2",
		MessageType:     messages.AuthTag,
		Timestamp:       &currentTime,
		ProtocolVersion: version.ProtocolVersion16,
		Data:            "exampleData2",
	}

	// Create a new publisher
	pb := NewSqsPublisher(stack.sess)
	go pb.Listen(ctx)

	// Send a message to the queue
	go func() {
		time.Sleep(1 * time.Second)
		pb.GetProducerChannel() <- PublisherMessage{
			QueueName: queueName,
			Message:   exampleMessage,
		}

		time.Sleep(1 * time.Second)
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
				s.FailNow("Not supported message")
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
