package go_scrapy

import (
	"crypto/sha1"
	"encoding/hex"
	mapset "github.com/deckarep/golang-set"
	"net/http"
)

type Scheduler interface {
	AddRequest(r *http.Request)
	AddResponse(r *http.Response)
	NextRequest() *http.Request
	NextResponse() *http.Response
}

type SchedulerConfig struct {
	ReqQueueLen  int
	RespQueueLen int
}

type DupeFilterScheduler struct {
	reqQueue       chan *http.Request
	respQueue      chan *http.Response
	reqFingerPrint mapset.Set
}

func NewDupeFilterScheduler(config *SchedulerConfig) *DupeFilterScheduler {
	scheduler := &DupeFilterScheduler{
		reqFingerPrint: mapset.NewSet(),
	}
	reqQueueLen := config.ReqQueueLen
	if 0 == reqQueueLen {
		reqQueueLen = 1
	}
	respQueueLen := config.RespQueueLen
	if 0 == respQueueLen {
		respQueueLen = 1
	}
	scheduler.reqQueue = make(chan *http.Request, reqQueueLen)
	scheduler.respQueue = make(chan *http.Response, respQueueLen)
	return scheduler
}

func (d *DupeFilterScheduler) requestFingerPrint(r *http.Request) string {
	sha1obj := sha1.New()
	sha1obj.Write([]byte(r.Method))
	sha1obj.Write([]byte(r.URL.String()))
	header := map[string][]string(r.Header)
	for key, values := range header {
		sha1obj.Write([]byte(key))
		for _, value := range values {
			sha1obj.Write([]byte(value))
		}
	}
	return hex.EncodeToString(sha1obj.Sum([]byte(nil)))
}

func (d *DupeFilterScheduler) AddRequest(r *http.Request) {
	fingerPrint := d.requestFingerPrint(r)
	if !d.reqFingerPrint.Contains(fingerPrint) {
		d.reqQueue <- r
		d.reqFingerPrint.Add(fingerPrint)
	}
}

func (d *DupeFilterScheduler) AddResponse(r *http.Response) {
	d.respQueue <- r
}

func (d *DupeFilterScheduler) NextRequest() *http.Request {
	return <-d.reqQueue
}

func (d *DupeFilterScheduler) NextResponse() *http.Response {
	return <-d.respQueue
}
