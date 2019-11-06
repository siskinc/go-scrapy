package go_scrapy

import "net/http"

type Spider interface {
	Parse(crawl *Engine, resp *http.Response)
}
