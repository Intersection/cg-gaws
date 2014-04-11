// Package SQS provides a way to interact with the AWS Simple Queue service.
package sqs

import (
	"encoding/xml"
	"fmt"
  "net/url"

	"github.com/controlgroup/gaws"
)

type SimpleQueueService struct {
    Endpoint string //The docs say http://sqs.us-west-2.amazonaws.com
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

//Building the MessageRequests
type MessageRequest struct {
    queueUrl *Queue
    Action  string
    Messagebody string
    Version string
    SignatureMethod string
    Expires string
    AWSAccessKey string //how do we take this out and use tokens instead for the URL? Is the AccessKey even required?
    SignatureVersion  string
    Signature
}

//Not sure this is correct - the QueueUrl should be exactly what was sent back from AWS - not created
type Queue struct {
    Queueurl string
}

//Building the QueueRequests
type QueueRequest struct {
    service *SimpleQueueService
    Action  string
    Messagebody string
    Version string
    SignatureMethod string
    Expires string
    AWSAccessKey string //how do we take this out and use tokens instead for the URL? Is the AccessKey even required?
    SignatureVersion  string
    Signature string
}
    

//builds the request for SQS - but Messge reuests look different from Queue requests
// 
//The below is a createQueueRequest
func (q *QueueRequest) createQueueRequest(queueName string, attribute map[string]) gaws.AWSRequest {
    resp = &CreateQueueResponse{}
    i := 1
    for n, v := range attribute {
       //Assign the name and value for each passed in attribute for Attribute.i.Name, Attribute.i.Value 
    
  return r
}

//Store the QueueUrl as it is returned from CreateQueueReponse as the components can change 
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

type SendMessageResponse struct {
    MD5OfMessageBody string `xml:"SendMessageResult>MD5OfMessageBody"`
    MessageId string `xml:"SendMessageResult>MessageId"`
    RequestId string `xml:"ResponseMetaData>RequestId"`
}     

func (sqs *SimpleQueueService) CreateQueue(name string) (q *Queue, err error) {
    create, err := sqs.CreateQueue(name)
    if err != nil {
      return nil, err
    }
    q := &Queue{sqs, create.QueueUrl}
    return q
}
