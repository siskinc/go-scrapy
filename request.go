package go_scrapy

import (
	"io"
	"net/http"
)

type Request struct {
	HttpRequest    *http.Request
	CustomerConfig map[string]interface{}   // 自定义参数
	Parse          func(*Engine, *Response) // 用来处理request对应的response的parse函数，不填会直接走默认的parse
}

func NewRequest(method, url string, body io.Reader, config map[string]interface{}) (req *Request, err error) {
	req = &Request{
		CustomerConfig: config,
	}
	req.HttpRequest, err = http.NewRequest(method, url, body)
	return
}
