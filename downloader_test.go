package go_scrapy

import (
	"github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestNewDownloader(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	downloader := NewDownloader(&DownloaderConfig{
		RetryMax:      10,
		WorkerNumber:  10,
		RetrySleep:    10 * time.Millisecond,
		RequestNumber: 100,
	})
	go downloader.run()
	testURL, _ := url.Parse("https://www.baidu.com")
	for i := 0; i < 100; i++ {
		downloader.AddRequest(&http.Request{
			Method: http.MethodGet,
			URL:    testURL,
		})
		logrus.Debug(i)
	}
	//var waitChan chan struct{}
	//<-waitChan
}
