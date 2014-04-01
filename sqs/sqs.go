// Package SQS provides a way to interact with the AWS Simple Queue service.
package sqs

import (
	"encoding/xml"
	"fmt"

	"github.com/controlgroup/gaws"
)

type SimpleQueueService struct {
    Endpoint string //The docs say http://sqs.us-east-1.amazonaws.com
}

// message is a SQS message. These are put onto or received from a defined Queue.
type Message struct {
	  Attribute     []Attribute  //String to string map 
	  Body          string `xml:"ReceiveMessageResult>Message>Body"` // String of message contents - not url-encoded
	  MD5OfBody     string `xml:"ReceiveMessageResult>Message>MD5OfBody"` //MD5 digest of the non-url-encoded message body string
    MessageId     string `xml:"ReceiveMessageResult>Message>MessageId"` // unique identifier for the message - considered unique across all AWS accounts for a period of time
    ReceiptHandle string `xml:"ReceiveMessageResult>Message>ReceiptHandle"` // identifier associated with the act of receiving the message - new handle returned everytime receive a message
}

type Attribute struct {
    Name  string `xml:"ReceiveMessageResult>Message>Attribute>Name"` //name if the attribute - ex. Policy, ApproximateNumberOfMessages, LastModifiedTimestamp
    Value string `xml:"ReceiveMessageResult>Message>Attribute>Value"` //the value of the attribute
}

type CreateQueueResponse struct {
    QueueUrl  string `xml:"CreateQueueResult>QueueUrl"`
}

type GetQueueAttributesResponse struct {
    Attribute []Attribute 
}

type DeleteMessageResponse struct {
    RequestId string `xml:"ResponseMetaData>RequestId"`
}

type GetQueueUrlResponse struct { 
    QueueUrl  string  `xml:"GetQueueUrlResult>QueueUrl"`
    RequestId string  `xml:"ResponseMetaData>RequestId"`
}

type ListQueueResponse struct {
    QueueUrl  string `xml:"ListQueuesResult>QueueUrl"`
    RequestId string  `xml:"ResponseMetaData>RequestId"`
}

type ReceiveMessageResponse struct {
    Messages []Messages `xml:"ReceiveMessageResult>Message"`
    RequestId string  `xml:"ResponseMetaData>RequestId"`
}
