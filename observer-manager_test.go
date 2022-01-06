package sqsObserver_go

import (
	"context"
	"github.com/GLCharge/sqsObserver-go/models/messages"
	"github.com/lorenzodonini/ocpp-go/ocpp1.6/core"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type (
	ObserverMock struct {
		mock.Mock
		hasStarted bool
		queues     []string
	}

	PublisherMock struct {
		mock.Mock
		isListening bool
	}

	ObserverManagerTestSuite struct {
		suite.Suite
		publisher       *PublisherMock
		observer1       *ObserverMock
		observer2       *ObserverMock
		observerManager ObserverManager
	}
)

/*------------------- Observer mock ------------------------------*/
func (m *ObserverMock) Start(ctx context.Context) {
	m.hasStarted = true
	m.Called(ctx)
}

func (m *ObserverMock) GetConsumerChannel() <-chan messages.ApiMessage {
	args := m.Called()
	return args.Get(0).(<-chan messages.ApiMessage)
}

func (m *ObserverMock) AddQueuesToObserve(queueNames ...string) {
	m.queues = append(m.queues, queueNames...)
	m.Called(queueNames)
}

func (m *ObserverMock) SetPollDuration(pollDuration int64) {
	m.Called(pollDuration)
}

func (m *ObserverMock) SetTimeout(timeout int64) {
	m.Called(timeout)
}

/*------------------- Publisher mock ------------------------------*/

func (p *PublisherMock) Send(queue string, message messages.ApiMessage) error {
	args := p.Called(queue, message)
	return args.Error(0)
}

func (p *PublisherMock) Listen(ctx context.Context) {
	p.Called(ctx)
	p.isListening = true
}

func (p *PublisherMock) GetProducerChannel() chan<- PublisherMessage {
	args := p.Called()
	return args.Get(0).(chan<- PublisherMessage)
}

/*------------------- Test Suite ------------------------------*/

func (suite *ObserverManagerTestSuite) SetupTest() {
	suite.observerManager = GetObserverManager()
	observerChannel := make(chan messages.ApiMessage)
	producerChannel := make(chan messages.ApiMessage)

	suite.publisher = new(PublisherMock)
	// Setup expectations
	suite.publisher.On("Send", "q1", messages.ApiMessage{}).Return(nil)
	suite.publisher.On("Listen", context.TODO()).Return()
	suite.publisher.On("GetProducerChannel", core.ReasonLocal).Return(producerChannel)

	suite.observer1 = new(ObserverMock)
	// Setup expectations
	suite.observer1.On("Start", context.TODO()).Return()
	suite.observer1.On("GetConsumerChannel").Return(observerChannel)
	suite.observer1.On("AddQueuesToObserve", "q1", "q2", "q3").Return()
	suite.observer1.On("SetPollDuration", 123).Return()
	suite.observer1.On("SetTimeout", 123).Return()

	suite.observer2 = new(ObserverMock)
	// Setup expectations
	suite.observer2.On("Start", context.TODO()).Return()
	suite.observer2.On("GetConsumerChannel").Return(observerChannel)
	suite.observer2.On("AddQueuesToObserve", "q6", "q4", "q5").Return()
	suite.observer2.On("SetPollDuration", 1).Return()
	suite.observer2.On("SetTimeout", 1).Return()
}

func (suite *ObserverManagerTestSuite) TestAddObserver() {
	suite.observerManager.AddObserver("o1", suite.observer1)
	suite.observerManager.AddObserver("o2", suite.observer2)

	suite.Require().Equal(suite.observer1, suite.observerManager.GetMultipleObserver("o1"))
	suite.Require().Equal(suite.observer2, suite.observerManager.GetMultipleObserver("o2"))
}

func (suite *ObserverManagerTestSuite) TestPublisher() {
	suite.observerManager.SetDefaultPublisher(suite.publisher)

	suite.Require().Equal(suite.publisher, suite.observerManager.GetDefaultPublisher())
}

func (suite *ObserverManagerTestSuite) TestStartObservers() {
	suite.observerManager.AddObserver("o1", suite.observer1)
	suite.observerManager.AddObserver("o2", suite.observer2)

	ctx := context.TODO()
	suite.observerManager.StartObservers(ctx)

	time.Sleep(time.Millisecond * 100)
	suite.observer1.AssertCalled(suite.T(), "Start", ctx)
	suite.observer2.AssertCalled(suite.T(), "Start", ctx)
}

func (suite *ObserverManagerTestSuite) TestHasObserverWithTag() {
	suite.observerManager.AddObserver("o1", suite.observer1)
	suite.Require().True(suite.observerManager.HasObserverWithTag("o1"))

	suite.observerManager.AddObserver("o2", suite.observer2)
	suite.Require().True(suite.observerManager.HasObserverWithTag("o2"))

	suite.Require().False(suite.observerManager.HasObserverWithTag("o23"))
}

func (suite *ObserverManagerTestSuite) TestGetObservers() {
	suite.observerManager.AddObserver("o1", suite.observer1)
	suite.observerManager.AddObserver("o2", suite.observer2)

	suite.Require().Contains(suite.observerManager.GetObservers(), suite.observer1)
	suite.Require().Contains(suite.observerManager.GetObservers(), suite.observer2)
}

func TestObserverManager(t *testing.T) {
	suite.Run(t, new(ObserverManagerTestSuite))
}
