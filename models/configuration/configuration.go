package configuration

type (
	SQS struct {
		PollDuration   int64      `fig:"pollDuration" default:"3"`
		MessageTimeout int64      `fig:"messageTimeout" default:"2"`
		Queues         []Observer `fig:"queues"`
	}

	Observer struct {
		Tag            string `fig:"tag"`
		QueueName      string `fig:"queueName" validation:"required"`
		PollDuration   int64  `fig:"pollDuration" validation:"gte=0"`
		MessageTimeout int64  `fig:"messageTimeout" default:"2"`
	}
)
