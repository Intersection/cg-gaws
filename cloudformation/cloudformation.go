package cloudformation

import (
	"fmt"
	//"encoding/xml"
	"github.com/controlgroup/gaws"
)

type CloudFormationService gaws.AWSService

type Template struct {
	Service *CloudFormationService
}

type validateTemplateRequest struct {
	TemplateBody string `xml:",omitempty"`
	TemplateURL  string `xml:",omitempty"`
}

func (t *Template) ValidateTemplate(template validateTemplateRequest) {
	url := t.Service.Endpoint
	fmt.Println(url)
}
