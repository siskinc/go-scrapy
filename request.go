package go_scrapy

import (
	"io"
	"net/http"
)

type Request struct {
	HttpRequest *http.Request
	Config      map[string]interface{} // 自定义参数
}

func NewRequest(method, url string, body io.Reader, config map[string]interface{}) (req *Request, err error) {
	req = &Request{
		Config: config,
	}
	req.HttpRequest, err = http.NewRequest(method, url, body)
	return
}
