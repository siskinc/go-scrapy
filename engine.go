package go_scrapy

import (
	"errors"
	"github.com/sirupsen/logrus"
)

type EngineConfig struct {
	DownloaderConfig *DownloaderConfig
}

type Engine struct {
	downloader   *Downloader
	scheduler    Scheduler
	spider       Spider
	pipelines    []ItemPipeline
	StartUrlList []*Request
	KeepRun      chan struct{}
}

func NewEngine(config *EngineConfig) *Engine {
	engine := &Engine{}
	engine.downloader = NewDownloader(config.DownloaderConfig)
	schedulerConfig := &SchedulerConfig{
		ReqQueueLen:  config.DownloaderConfig.RequestNumber,
		RespQueueLen: config.DownloaderConfig.RequestNumber,
	}
	engine.scheduler = NewDupeFilterScheduler(schedulerConfig)
	return engine
}

func (e *Engine) AddRequest(r *Request) {
	e.scheduler.AddRequest(r)
}

func (e *Engine) AddResponse(r *Response) {
	e.scheduler.AddResponse(r)
}

func (e *Engine) AddItem(item interface{}) {
	for _, pipeline := range e.pipelines {
		err := pipeline.ProcessItem(item, e.spider)
		if errors.Is(err, DropItemErr) {
			break
		}
	}
}

func (e *Engine) RegisterSpider(spider Spider) {
	e.spider = spider
}

func (e *Engine) RegisterPipeline(pipelines ...ItemPipeline) {
	for i := range pipelines {
		e.pipelines = append(e.pipelines, pipelines[i])
	}
}

func (e *Engine) Start() {
	if e.spider == nil {
		logrus.Fatalf("spider not register!")
	}
	for i := range e.StartUrlList {
		e.AddRequest(e.StartUrlList[i])
	}
	go e.downloader.run()
	go func() {
		for {
			req := e.scheduler.NextRequest()
			e.downloader.AddRequest(req)
		}
	}()
	go func() {
		for {
			resp := e.scheduler.NextResponse()
			e.spider.Parse(e, resp)
		}
	}()
	go func() {
		for {
			resp := e.downloader.GetResponse()
			e.scheduler.AddResponse(resp)
		}
	}()
	<-e.KeepRun
}
