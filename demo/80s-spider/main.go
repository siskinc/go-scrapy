package main

import (
	"github.com/sirupsen/logrus"
	go_scrapy "github.com/siskinc/go-scrapy"
	"github.com/siskinc/go-scrapy/demo/80s-spider/pipelines"
	"github.com/siskinc/go-scrapy/demo/80s-spider/spider"
	"net/http"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	downloaderConfig := &go_scrapy.DownloaderConfig{
		RetryMax:       3,
		RetrySleep:     5,
		WorkerNumber:   5,
		RequestNumber:  10,
		ResponseNumber: 100,
	}
	engineConfig := &go_scrapy.EngineConfig{DownloaderConfig: downloaderConfig}
	engine := go_scrapy.NewEngine(engineConfig)
	movieUrl, err := http.NewRequest(http.MethodGet, "http://8080s.net/movie/list", nil)
	if nil != err {
		panic(err)
	}
	engine.StartUrlList = []*go_scrapy.Request{
		{
			movieUrl,
			map[string]interface{}{
				spider.ConfigUrlType: spider.UrlTypeMovie,
				spider.ConfigUrlInfo: spider.ConfigUrlInfoPage,
			},
		},
	}
	engine.RegisterSpider(&spider.Movie80sSpider{})
	engine.RegisterPipeline(&pipelines.VideoPipeline{})
	engine.Start()
}
