package go_scrapy

import (
	"errors"
	"github.com/sirupsen/logrus"
	"time"
)

type EngineConfig struct {
	DownloaderConfig *DownloaderConfig
}

type Engine struct {
	Downloader   *Downloader
	scheduler    Scheduler
	spider       Spider
	pipelines    []ItemPipeline
	StartUrlList []*Request
	KeepRun      chan struct{}
}

func NewEngine(config *EngineConfig) *Engine {
	engine := &Engine{}
	engine.Downloader = NewDownloader(config.DownloaderConfig)
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
	go e.Downloader.run()
	go func() {
		for {
			req := e.scheduler.NextRequest()
			logrus.Debugf("get request from scheduler to Downloader, url: %s, method: %s", req.HttpRequest.URL, req.HttpRequest.Method)
			e.Downloader.AddRequest(req)
			time.Sleep(time.Nanosecond)
		}
	}()
	go func() {
		for {
			resp := e.scheduler.NextResponse()
			logrus.Debugf("get response from scheduler to spider, url: %s, method: %s", resp.HttpResponse.Request.URL,
				resp.HttpResponse.Request.Method)
			go e.spider.Parse(e, resp)
			time.Sleep(time.Nanosecond)
		}
	}()
	go func() {
		for {
			resp := e.Downloader.GetResponse()
			logrus.Debugf("get response from Downloader to scheduler, url: %s, method: %s", resp.HttpResponse.Request.URL,
				resp.HttpResponse.Request.Method)
			e.scheduler.AddResponse(resp)
			time.Sleep(time.Nanosecond)
		}
	}()
	<-e.KeepRun
}
