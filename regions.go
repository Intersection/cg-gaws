package gaws

// Region is the name of the default region for gaws to use.
var Region string = "us-east-1"

// AWSService is a representation of an AWS Service.
type AWSService struct {
	Endpoint string
}

var usEast1 = map[string]AWSService{
	"kinesis":  AWSService{Endpoint: "https://kinesis.us-east-1.amazonaws.com"},
	"dynamodb": AWSService{Endpoint: "https://dynamodb.us-east-1.amazonaws.com"},
}

var regionsToServices = map[string]map[string]AWSService{
	"us-east-1": usEast1,
}

var noSuchServiceError = AWSError{Type: "GawsNoSuchService", Message: "Could not find this service."}

// ServiceForRegion will return the AWSService for a given region and service name.
func ServiceForRegion(region string, serviceName string) (AWSService, error) {
	service := regionsToServices[region][serviceName]

	if service.Endpoint == "" {
		return service, noSuchServiceError
	}

	return service, nil
}
