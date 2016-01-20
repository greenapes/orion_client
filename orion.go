package orion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Attribute struct {
	Type  string
	Value string
}

type Attributes struct {
	values map[string]Attribute
}

func NewAttributes() Attributes {
	return Attributes{
		values: make(map[string]Attribute),
	}
}

func (self Attributes) Add(name string, value interface{}) error {
	attr := Attribute{}
	switch el := value.(type) {
	case string:
		attr.Type = "string"
		attr.Value = el
	case int:
		attr.Type = "int"
		attr.Value = strconv.FormatInt(int64(el), 10)
	case int64:
		attr.Type = "int"
		attr.Value = strconv.FormatInt(el, 10)
	case Attribute:
		attr = el
	default:
		return fmt.Errorf("unsupported implicit type")
	}

	self.values[name] = attr
	return nil
}

func (self Attributes) toWire() wireAttributes {
	var attrs []wireAttribute
	for key, value := range self.values {
		attr := wireAttribute{
			Name:  key,
			Type:  value.Type,
			Value: value.Value,
		}
		attrs = append(attrs, attr)
	}
	return wireAttributes{attrs}
}

type Entity interface {
	Id() string
	Type() string
	Attributes() Attributes
	SetAttributes(Attributes)
}

type EntityFactory func(etype, id string) Entity

type Page uint

func (self Page) Next() Page {
	return Page(uint(self) + 1)
}

type Server struct {
	server_url string
}

func NewServer(u string) *Server {
	return &Server{
		strings.TrimSuffix(u, "/"),
	}
}

type wireAttribute struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type wireAttributes struct {
	Attributes []wireAttribute `json:"attributes"`
}

func (self wireAttributes) toAttributes() Attributes {
	attrs := NewAttributes()
	for _, el := range self.Attributes {
		attrs.Add(el.Name, Attribute{
			Type:  el.Type,
			Value: el.Value,
		})
	}
	return attrs
}

type wireId struct {
	Id        string `json:"id"`
	IsPattern bool   `json:"isPattern,string"`
	Type      string `json:"type"`
}

type wireStatus struct {
	Code    uint   `json:"code,string"`
	Message string `json:"reasonPhrase"`
}

type wireAlteredContextElement struct {
	wireAttributes
	wireStatus `json:"statusCode"`
}

type wireAlteredContextResponse struct {
	Elements []wireAlteredContextElement `json:"contextResponses"`
	wireId
}

type wireQueryContextElement struct {
	ContextElement struct {
		wireAttributes
		wireId
	} `json:"contextElement"`
	wireStatus `json:"statusCode"`
}

type wireQueryContextResponse struct {
	Elements []wireQueryContextElement `json:"contextResponses"`
}

func (self *Server) NewEntity(e Entity) error {
	var result wireAlteredContextResponse

	u := fmt.Sprintf("/v1/contextEntities/type/%s/id/%s", e.Type(), e.Id())
	err := self.post(u, e.Attributes().toWire(), &result)
	if err != nil {
		return err
	}

	status := result.Elements[0]
	if status.Code != 200 {
		return fmt.Errorf("entity creation failed. code=%d message=%s", status.Code, status.Message)
	}

	return nil
}

func (self *Server) EntitiesByType(entity_type string, page Page, f EntityFactory) ([]Entity, error) {
	limit := int64(100)
	offset := int64(page) * limit
	u := fmt.Sprintf("/v1/contextEntityTypes/%s?limit=%s&offset=%s",
		entity_type,
		url.QueryEscape(strconv.FormatInt(limit, 10)),
		url.QueryEscape(strconv.FormatInt(offset, 10)))

	result := wireQueryContextResponse{}
	err := self.get(u, &result)
	if err != nil {
		return nil, err
	}

	var output []Entity
	for _, el := range result.Elements {
		ctx := &el.ContextElement
		entity := f(ctx.Type, ctx.Id)
		entity.SetAttributes(ctx.wireAttributes.toAttributes())
		output = append(output, entity)
	}
	return output, nil
}

func (self *Server) AllEntitiesByType(entity_type string, f EntityFactory) ([]Entity, error) {
	var output []Entity
	page := Page(0)
	for {
		chunk, err := self.EntitiesByType(entity_type, page, f)
		if err != nil {
			return output, err
		}
		if len(chunk) == 0 {
			break
		}
		for _, el := range chunk {
			output = append(output, el)
		}
		page = page.Next()
	}
	return output, nil
}

func (self *Server) EntityById(e Entity) error {
	u := fmt.Sprintf("/v1/contextEntities/type/%s/id/%s", e.Type(), e.Id())

	result := wireQueryContextElement{}
	err := self.get(u, &result)
	if err != nil {
		return err
	}

	if result.Code != 200 {
		return fmt.Errorf("entity lookup failed. code=%d message=%s", result.Code, result.Message)
	}

	e.SetAttributes(result.ContextElement.wireAttributes.toAttributes())

	return nil
}

func (self *Server) get(path string, response interface{}) error {
	u := self.server_url + path
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	return self.do(req, response)
}

func (self *Server) post(path string, body interface{}, response interface{}) error {
	octets, err := json.Marshal(body)
	if err != nil {
		return err
	}

	u := self.server_url + path
	req, err := http.NewRequest("POST", u, bytes.NewReader(octets))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return self.do(req, response)
}

func (self *Server) do(req *http.Request, response interface{}) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	octets, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(octets, response)
	if err != nil {
		return err
	}
	return nil
}
