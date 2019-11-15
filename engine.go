package go_scrapy

import (
	"errors"
	"net/http"
)

type EngineConfig struct {
	DownloaderConfig *DownloaderConfig
}

type Engine struct {
	downloader *Downloader
	scheduler  Scheduler
	spider     Spider
	pipelines  []ItemPipeline
}

func NewEngine(config *EngineConfig) *Engine {
	engine := &Engine{}
	engine.downloader = NewDownloader(config.DownloaderConfig)
	return engine
}

func (e *Engine) AddRequest(r *http.Request) {
	e.scheduler.AddRequest(r)
}

func (e *Engine) AddResponse(r *http.Response) {
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
}
