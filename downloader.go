package go_scrapy

import (
	"errors"
	"github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type DownloadMiddleWare interface {
	ProcessRequest(*http.Request) (*http.Request, *http.Response, error)
	ProcessResponse(*http.Response) (*http.Request, *http.Response, error)
}

type DownloaderConfig struct {
	RetryMax       int
	RetrySleep     time.Duration
	WorkerNumber   int
	RequestNumber  uint64
	ResponseNumber uint64
}

type Downloader struct {
	client       *http.Client
	requests     chan *http.Request
	responses    chan *http.Response
	runWorkLimit chan struct{}
	retry        int
	retrySleep   time.Duration
	middleWares  []DownloadMiddleWare
}

var (
	IgnoreRequest  = errors.New("ignore this request")
	IgnoreResponse = errors.New("ignore this response")
)

func NewDownloader(config *DownloaderConfig) *Downloader {
	downloader := &Downloader{
		retry:      config.RetryMax,
		retrySleep: config.RetrySleep,
	}
	if config.WorkerNumber == 0 {
		config.WorkerNumber = 1
	}
	if config.RequestNumber == 0 {
		config.RequestNumber = uint64(config.WorkerNumber)
	}
	if config.ResponseNumber == 0 {
		config.ResponseNumber = uint64(config.WorkerNumber)
	}
	downloader.requests = make(chan *http.Request, config.RequestNumber)
	downloader.responses = make(chan *http.Response, config.ResponseNumber)
	downloader.runWorkLimit = make(chan struct{}, config.WorkerNumber)
	downloader.client = http.DefaultClient
	return downloader
}

func (d *Downloader) RegisterMiddleWare(middleWares ...DownloadMiddleWare) {
	d.middleWares = append(d.middleWares, middleWares...)
}

func (d *Downloader) SetClient(client *http.Client) {
	d.client = client
}

func (d *Downloader) GetResponse() *http.Response {
	return <-d.responses
}

func (d *Downloader) run() {
	for req := range d.requests {
		d.runWorkLimit <- struct{}{}
		beginTime := time.Now().Unix()
		go d.download(req)
		endTime := time.Now().Unix()
		logrus.Debugf("Download %s %s is successful, time cost: %d", req.Method, req.URL,
			endTime-beginTime)
	}
}

func (d *Downloader) AddRequest(req *http.Request) {
	d.requests <- req
}

func (d *Downloader) HandleMiddleWare(req *http.Request, resp *http.Response) (skip bool) {
	skip = false
	if req != nil {
		d.AddRequest(req)
		skip = true
	}
	if resp != nil {
		d.responses <- resp
		skip = true
	}
	return
}

func (d *Downloader) download(req *http.Request) {
	retry := 0
	defer func() {
		<-d.runWorkLimit
	}()
	var request *http.Request
	var response *http.Response
	var err error
	for _, middleWare := range d.middleWares {
		request, response, err = middleWare.ProcessRequest(req)
		if err != nil {
			if !errors.Is(err, IgnoreRequest) {
				logrus.Errorf("%s %s in download middleware(%+v) is err: %v.",
					req.Method, middleWare, req.URL, err)
			}
			return
		}
		if d.HandleMiddleWare(request, response) {
			logrus.Debugf("break in middleware: %v.", middleWare)
			return
		}
	}
	var resp *http.Response
	for retry <= d.retry {
		if retry > 1 {
			logrus.Debugf("%s %s retry count: %d.", req.Method, req.URL, retry)
		}
		resp, err = d.client.Do(req)
		if err != nil {
			logrus.Errorf("%s %s is err: %v.", req.Method, req.URL, err)
		}
		if resp.StatusCode >= 400 {
			logrus.Errorf("%s %s is failed, status code is: %d.", req.Method, req.URL, resp.StatusCode)
		} else {
			break
		}
		retry++
		if d.retrySleep > 0 {
			logrus.Infof("%s %s retry count: %d, sleep: %v.", req.Method, req.URL, retry, d.retrySleep)
			time.Sleep(d.retrySleep)
		} else {
			logrus.Infof("%s %s retry count: %d, don't sleep.", req.Method, req.URL, retry)
		}
	}
	if retry > d.retry {
		logrus.Errorf("%s %s is retry max", request.Method, request.URL)
		return
	}
	for _, middleWare := range d.middleWares {
		request, response, err = middleWare.ProcessResponse(resp)
		if err != nil {
			if !errors.Is(err, IgnoreResponse) {
				logrus.Errorf("%s %s in download middleware(%+v) is err: %v",
					req.Method, middleWare, req.URL, err)
			}
			return
		}
		if d.HandleMiddleWare(request, response) {
			logrus.Debugf("break in middleware: %v", middleWare)
			return
		}
	}
}
