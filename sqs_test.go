package sqsObserver_go

import (
	"context"
	"fmt"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/GLCharge/sqsObserver-go/models/version"
	"github.com/GLCharge/sqsObserver-go/sqs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cenkalti/backoff/v4"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
	assert2 "github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

const (
	localstackRepository       = "localstack/localstack"
	localstackTag              = "latest"
	localstackPort             = "4566/tcp"
	awsID, awsSecret, awsToken = "abc", "def", "ghi"
	backoffInterval            = 100 * time.Millisecond
)

var queueName = "HeartbeatQueue"

type (
	// LocalstackConfig is a configuration for Localstack.
	LocalstackConfig struct {
		Region                     string
		Services                   []string
		ContainerExpirationSeconds int
		BackoffDuration            time.Duration
	}
	// Localstack is a Localstack docker container.
	Localstack struct {
		sess *session.Session
		b    backoff.BackOff
	}
)

// NewLocalstack creates a new Localstack container and allows
// creation of streams, tables and other AWS resources.
func NewLocalstack(cfg LocalstackConfig) (*Localstack, error) {
	container, err := createContainer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	sess, err := createSession(cfg.Region, container.GetPort(localstackPort))
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	b := backoff.NewConstantBackOff(cfg.BackoffDuration)
	b.Interval = backoffInterval
	return &Localstack{
		sess: sess,
		b:    b,
	}, nil
}

func createContainer(cfg LocalstackConfig) (*dockertest.Resource, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("failed to create docker pool: %w", err)
	}

	container, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: localstackRepository,
			Tag:        localstackTag,
			Env: []string{
				"SERVICES=" + strings.Join(cfg.Services, ","),
				"DEFAULT_REGION=" + cfg.Region,
				"START_WEB=0",
				"DOCKER_HOST=unix:///var/run/docker.sock",
				"DATA_DIR=/tmp/localstack/data",
			},
			Mounts: []string{
				"/var/run/docker.sock:/var/run/docker.sock",
			},
		},
		func(c *docker.HostConfig) {
			c.AutoRemove = true
			c.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	if err = container.Expire(uint(cfg.ContainerExpirationSeconds)); err != nil {
		return nil, fmt.Errorf("failed to expire container: %w", err)
	}

	return container, nil
}

func createSession(region, port string) (*session.Session, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:                        aws.String(region),
		Endpoint:                      aws.String("localhost:" + port),
		Credentials:                   credentials.NewStaticCredentials(awsID, awsSecret, awsToken),
		DisableSSL:                    aws.Bool(true),
		CredentialsChainVerboseErrors: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new aws session: %w", err)
	}
	return sess, nil
}

func TestSQS(t *testing.T) {
	assert := assert2.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	// Create a new AWS stack
	localstack, err := NewLocalstack(LocalstackConfig{
		Region:                     "eu-central-1",
		Services:                   []string{"sqs"},
		ContainerExpirationSeconds: 100,
		BackoffDuration:            100,
	})
	assert.NoError(err)

	time.Sleep(5 * time.Second)

	svc := awsSqs.New(localstack.sess)

	// Create queues
	_, err = sqs.CreateQueue(svc, &queueName)
	assert.NoError(err)

	// Create a new observer
	testObserver := NewSqsSingleObserver(localstack.sess, queueName, 3)
	go testObserver.Start(ctx)

	// Create a new publisher
	pb := NewSqsPublisher(localstack.sess)
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
			assert.Equal(exampleMessage.MessageId, msg.MessageId)
			assert.Equal(exampleMessage.Data, msg.Data)
			assert.Equal(exampleMessage.MessageType, msg.MessageType)

			cancel()
			break
		}
	}
}
