package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
)

//CreateQueue creates a new queue with a name
func CreateQueue(svc *awsSqs.SQS, queueName *string) (*awsSqs.CreateQueueOutput, error) {
	result, err := svc.CreateQueue(&awsSqs.CreateQueueInput{
		QueueName: queueName,
		Attributes: map[string]*string{
			"MessageRetentionPeriod": aws.String("86400"),
		},
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

//SendMsg Sends a message to the queue.
func SendMsg(svc *awsSqs.SQS, queueURL *string, messageAttributes map[string]*awsSqs.MessageAttributeValue, messageBody string) error {
	_, err := svc.SendMessage(&awsSqs.SendMessageInput{
		DelaySeconds:      aws.Int64(0),
		MessageAttributes: messageAttributes,
		MessageBody:       &messageBody,
		QueueUrl:          queueURL,
	})

	return err
}

//GetMessages polls the SQS for messages from a queue.
func GetMessages(svc *awsSqs.SQS, queueURL string, timeout, waitTime *int64) (*awsSqs.ReceiveMessageOutput, error) {
	msgResult, err := svc.ReceiveMessage(&awsSqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(awsSqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(awsSqs.QueueAttributeNameAll),
		},
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(5),
		VisibilityTimeout:   timeout,
		WaitTimeSeconds:     waitTime,
	})

	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// GetQueueURL gets  a URL for a Queue
func GetQueueURL(svc *awsSqs.SQS, queue string) (*awsSqs.GetQueueUrlOutput, error) {
	urlResult, err := svc.GetQueueUrl(&awsSqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return urlResult, nil
}

// GetQueueURLFromSession gets  a URL for a Queue
func GetQueueURLFromSession(session *session.Session, queue string) (*awsSqs.GetQueueUrlOutput, error) {
	svc := awsSqs.New(session)
	urlResult, err := svc.GetQueueUrl(&awsSqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return urlResult, nil
}

func DeleteMessage(svc *awsSqs.SQS, queueURL *string, messageHandle *string) error {
	_, err := svc.DeleteMessage(&awsSqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: messageHandle,
	})

	return err
}
