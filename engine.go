package go_scrapy

import (
	"errors"
	"time"

	"github.com/sirupsen/logrus"
)

type EngineConfig struct {
	DownloaderConfig *DownloaderConfig
	MaxParseWorker   int
}

type Engine struct {
	Downloader     *Downloader
	Scheduler      Scheduler
	spider         Spider
	pipelines      []ItemPipeline
	StartUrlList   []*Request
	KeepRun        chan struct{}
	respCache      chan *Response
	maxParseWorker int
}

func ParseWorker(e *Engine, workID int) {
	for resp := range e.respCache {
		logrus.Debug("work id: %d, parse %s", workID, resp.HttpResponse.Request.URL.String())
		if resp.Parse != nil {
			resp.Parse(e, resp)
		} else {
			e.spider.Parse(e, resp)
		}
	}
}

func NewEngine(config *EngineConfig) *Engine {
	engine := &Engine{}
	engine.Downloader = NewDownloader(config.DownloaderConfig)
	schedulerConfig := &SchedulerConfig{
		ReqQueueLen:  config.DownloaderConfig.RequestNumber,
		RespQueueLen: config.DownloaderConfig.RequestNumber,
	}
	if config.MaxParseWorker == 0 {
		config.MaxParseWorker = 10
	}
	engine.maxParseWorker = config.MaxParseWorker
	engine.Scheduler = NewDupeFilterScheduler(schedulerConfig)
	return engine
}

func (e *Engine) AddRequest(r *Request) {
	e.Scheduler.AddRequest(r)
}

func (e *Engine) AddResponse(r *Response) {
	e.Scheduler.AddResponse(r)
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
	for i := 0; i < e.maxParseWorker; i++ {
		go ParseWorker(e, i+1)
	}
	go e.Downloader.run()
	go func() {
		for {
			req := e.Scheduler.NextRequest()
			logrus.Debugf("get request from Scheduler to Downloader, url: %s, method: %s", req.HttpRequest.URL, req.HttpRequest.Method)
			e.Downloader.AddRequest(req)
			time.Sleep(time.Nanosecond)
		}
	}()
	go func() {
		for {
			resp := e.Scheduler.NextResponse()
			logrus.Debugf("get response from Scheduler to spider, url: %s, method: %s", resp.HttpResponse.Request.URL,
				resp.HttpResponse.Request.Method)
			e.respCache <- resp
		}
	}()
	go func() {
		for {
			resp := e.Downloader.GetResponse()
			logrus.Debugf("get response from Downloader to Scheduler, url: %s, method: %s", resp.HttpResponse.Request.URL,
				resp.HttpResponse.Request.Method)
			e.Scheduler.AddResponse(resp)
			time.Sleep(time.Nanosecond)
		}
	}()
	<-e.KeepRun
}
