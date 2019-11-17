package go_scrapy

import "net/http"

type Request struct {
	HttpRequest *http.Request
	Config      map[string]interface{} // 自定义参数
}
