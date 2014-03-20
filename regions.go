package gaws

// Region is the name of the default region for gaws to use.
var Region string = "us-east-1"

// AWSService is a representation of an AWS Service.
type AWSService struct {
	Endpoint string
}
