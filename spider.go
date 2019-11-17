package go_scrapy

type Spider interface {
	Parse(crawl *Engine, resp *Response)
}
