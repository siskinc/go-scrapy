package pipelines

import (
	go_scrapy "github.com/siskinc/go-scrapy"
	"github.com/siskinc/go-scrapy/demo/80s-spider/worker"
	"time"
)

var dbWriteWorker *worker.DatabaseWriterWorker

func init() {
	dbWriteWorker = worker.NewDatabaseWorker(&worker.MysqlInfo{
		Username: "root",
		Password: "root",
		Host:     "127.0.0.1",
		Database: "resource_search_service",
		Table:    "videos",
		Port:     "3306",
	}, []string{"created_at", "updated_at", "name", "type", "bt_url", "src_url"}, 1, 5000, time.Second*3, 5000)
	go dbWriteWorker.Start()
}

type VideoItem struct {
	Title     string
	SrcUrl    string
	Synopsis  string
	OnlineUrl string
	LeadActor string
	BtUrl     string
	Type      uint64
}

type VideoPipeline struct{}

func (v VideoPipeline) OpenSpider(spider go_scrapy.Spider) {

}

func (v VideoPipeline) CloseSpider(spider go_scrapy.Spider) {

}

func (v VideoPipeline) FromCrawler(crawler *go_scrapy.Engine) {

}

func (v VideoPipeline) ProcessItem(item interface{}, spider go_scrapy.Spider) error {
	video, ok := item.(*VideoItem)
	if !ok {
		return nil
	}
	now := time.Now()
	dbWriteWorker.SqlInfoChan <- []interface{}{
		now,
		now,
		video.Title,
		video.Type,
		video.BtUrl,
		video.SrcUrl,
	}
	return nil
}
