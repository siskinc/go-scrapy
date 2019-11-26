package go_scrapy

import (
	"crypto/sha1"
	"encoding/hex"
	mapset "github.com/deckarep/golang-set"
)

type Scheduler interface {
	AddRequest(r *Request)
	AddResponse(r *Response)
	NextRequest() *Request
	NextResponse() *Response
}

type SchedulerConfig struct {
	ReqQueueLen  int
	RespQueueLen int
}

type DupeFilterScheduler struct {
	reqQueue       chan *Request
	respQueue      chan *Response
	reqFingerPrint mapset.Set
	NeedFilter     bool
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
	scheduler.reqQueue = make(chan *Request, reqQueueLen)
	scheduler.respQueue = make(chan *Response, respQueueLen)
	return scheduler
}

func (d *DupeFilterScheduler) requestFingerPrint(r *Request) string {
	if !d.NeedFilter {
		return ""
	}
	req := r.HttpRequest
	sha1obj := sha1.New()
	sha1obj.Write([]byte(req.Method))
	sha1obj.Write([]byte(req.URL.String()))
	header := map[string][]string(req.Header)
	for key, values := range header {
		sha1obj.Write([]byte(key))
		for _, value := range values {
			sha1obj.Write([]byte(value))
		}
	}
	return hex.EncodeToString(sha1obj.Sum([]byte(nil)))
}

func (d *DupeFilterScheduler) AddRequest(r *Request) {
	fingerPrint := d.requestFingerPrint(r)
	if !d.NeedFilter || (d.NeedFilter && !d.reqFingerPrint.Contains(fingerPrint)) {
		d.reqQueue <- r
		d.reqFingerPrint.Add(fingerPrint)
		return
	}
}

func (d *DupeFilterScheduler) AddResponse(r *Response) {
	d.respQueue <- r
}

func (d *DupeFilterScheduler) NextRequest() *Request {
	return <-d.reqQueue
}

func (d *DupeFilterScheduler) NextResponse() *Response {
	return <-d.respQueue
}
