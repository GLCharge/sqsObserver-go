package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

const (
	observerTag1 = "o1"
	observerTag2 = "o2"
)

type (
	observerMock struct {
		mock.Mock
		hasStarted bool
		queues     []string
	}

	publisherMock struct {
		mock.Mock
	}

	observerManagerTestSuite struct {
		suite.Suite
		publisher       *publisherMock
		observer1       *observerMock
		observer2       *observerMock
		observerManager ObserverManager
	}
)

/*------------------- Observer mock ------------------------------*/
func (m *observerMock) Start(ctx context.Context) error {
	m.hasStarted = true
	args := m.Called()
	return args.Error(0)
}

func (m *observerMock) SetChannel(channel chan messages.ApiMessage) {
	m.Called(channel)
}

func (m *observerMock) SetLogger(logger *log.Logger) {
	m.Called(logger)
}

func (m *observerMock) GetConsumerChannel() <-chan messages.ApiMessage {
	args := m.Called()
	return args.Get(0).(<-chan messages.ApiMessage)
}

func (m *observerMock) AddQueuesToObserve(queues ...Queue) error {
	return m.Called(queues).Error(0)
}

func (m *observerMock) SetDefaultPollDuration(pollDuration int64) {
	m.Called(pollDuration)
}

func (m *observerMock) SetDefaultTimeout(timeout int64) {
	m.Called(timeout)
}

/*------------------- Publisher mock ------------------------------*/

func (p *publisherMock) Send(queue string, message messages.ApiMessage) error {
	args := p.Called(queue, message)
	return args.Error(0)
}

func (p *publisherMock) Listen(ctx context.Context) {
	p.Called()
}

func (p *publisherMock) GetProducerChannel() chan<- PublisherMessage {
	args := p.Called()
	return args.Get(0).(chan<- PublisherMessage)
}

func (p *publisherMock) SetLogger(logger *log.Logger) {
	p.Called(logger)
}

/*------------------- Test Suite ------------------------------*/

func (suite *observerManagerTestSuite) SetupTest() {
	suite.observerManager = GetObserverManager()
	var (
		observerChannel = make(chan messages.ApiMessage)
		producerChannel = make(chan messages.ApiMessage)
	)

	suite.publisher = new(publisherMock)
	// Setup expectations
	suite.publisher.On("Send", mock.Anything, messages.ApiMessage{}).Return(nil)
	suite.publisher.On("Listen").Return()
	suite.publisher.On("GetProducerChannel", core.ReasonLocal).Return(producerChannel)

	suite.observer1 = new(observerMock)
	// Setup expectations
	suite.observer1.On("Start").Return(nil)
	suite.observer1.On("GetConsumerChannel").Return(observerChannel)
	suite.observer1.On("AddQueuesToObserve", "q1", "q2", "q3").Return()
	suite.observer1.On("SetDefaultPollDuration", 123).Return()
	suite.observer1.On("SetDefaultTimeout", 123).Return()

	suite.observer2 = new(observerMock)
	// Setup expectations
	suite.observer2.On("Start").Return(nil)
	suite.observer2.On("GetConsumerChannel").Return(observerChannel)
	suite.observer2.On("AddQueuesToObserve", "q6", "q4", "q5").Return()
	suite.observer2.On("SetDefaultPollDuration", 1).Return()
	suite.observer2.On("SetDefaultTimeout", 1).Return()
}

func (suite *observerManagerTestSuite) TestAddObserver() {
	suite.observerManager.AddObserver(observerTag1, suite.observer1)
	suite.observerManager.AddObserver(observerTag2, suite.observer2)

	suite.Require().Equal(suite.observer1, suite.observerManager.GetMultipleObserver(observerTag1))
	suite.Require().Equal(suite.observer2, suite.observerManager.GetMultipleObserver(observerTag2))
}

func (suite *observerManagerTestSuite) TestPublisher() {
	suite.observerManager.SetDefaultPublisher(suite.publisher)

	suite.Require().Equal(suite.publisher, suite.observerManager.GetDefaultPublisher())
}

func (suite *observerManagerTestSuite) TestStartObservers() {
	suite.observerManager.AddObserver(observerTag1, suite.observer1)
	suite.observerManager.AddObserver(observerTag2, suite.observer2)

	ctx := context.TODO()
	suite.observerManager.StartObservers(ctx)

	time.Sleep(time.Millisecond * 100)
	suite.observer1.AssertCalled(suite.T(), "Start")
	suite.observer2.AssertCalled(suite.T(), "Start")
}

func (suite *observerManagerTestSuite) TestHasObserverWithTag() {
	suite.observerManager.AddObserver(observerTag1, suite.observer1)
	suite.Require().True(suite.observerManager.HasObserverWithTag(observerTag1))

	suite.observerManager.AddObserver(observerTag2, suite.observer2)
	suite.Require().True(suite.observerManager.HasObserverWithTag(observerTag2))

	suite.Require().False(suite.observerManager.HasObserverWithTag("o23"))
}

func (suite *observerManagerTestSuite) TestGetObservers() {
	suite.observerManager.AddObserver(observerTag1, suite.observer1)
	suite.observerManager.AddObserver(observerTag2, suite.observer2)

	suite.Require().Contains(suite.observerManager.GetObservers(), suite.observer1)
	suite.Require().Contains(suite.observerManager.GetObservers(), suite.observer2)
}

func TestObserverManager(t *testing.T) {
	suite.Run(t, new(observerManagerTestSuite))
}
