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
type message struct {
	  Attribute     []Attribute  //String to string map 
	  Body          string  // String of message contents - not url-encoded
	  MD5OfBody     string  //MD5 digest of the non-url-encoded message body string
    MessageId     string  // unique identifier for the message - considered unique across all AWS accounts for a period of time
    ReceiptHandle string  // identifier associated with the act of receiving the message - new handle returned everytime receive a message
}

type Attribute struct {
    Name  string `xml:"ReceiveMessageResult>Message>Attribute>Name"` //name if the attribute - ex. Policy, ApproximateNumberOfMessages, LastModifiedTimestamp
    Value string `xml:ReceiveMessageResult>Message>Attribute>Value"` //the value of the attribute
}

