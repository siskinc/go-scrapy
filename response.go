package go_scrapy

import "net/http"

type Response struct {
	HttpResponse   *http.Response
	CustomerConfig map[string]interface{} // 自定义参数
	Parse          func(*Engine, *Response)
}
