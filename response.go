package go_scrapy

import "net/http"

type Response struct {
	HttpResponse *http.Response
	Config       map[string]interface{} // 自定义参数
}
