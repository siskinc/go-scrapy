package go_scrapy

import "errors"

var (
	DropItemErr = errors.New("drop item")
)

type ItemPipeline interface {
	OpenSpider(spider Spider)
	CloseSpider(spider Spider)
	FromCrawler(crawler *Engine)
	ProcessItem(item interface{}, spider Spider) error
}
