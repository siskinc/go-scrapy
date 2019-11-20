package spider

import (
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/sirupsen/logrus"
	"github.com/siskinc/go-scrapy"
	"github.com/siskinc/go-scrapy/demo/80s-spider/pipelines"
	"net/http"
	"strconv"
	"strings"
)

const (
	ConfigUrlType    = "type"
	ConfigUrlInfo    = "info"
	ConfigMovieTitle = "title"
	UrlPre           = "http://8080s.net"
)

const (
	UrlTypeMovie int = iota
	UrlTypeTeleplay
)

const (
	ConfigUrlInfoPage int = iota
	ConfigUrlInfoDetail
)

const (
	VideoTypeMovie    uint64 = iota + 1 // 电影
	VideoTypeTeleplay                   // 电视剧
	VideoTypeShort                      // 短视频
)

type Movie80sSpider struct {
	PageNumber uint64
}

func (spider *Movie80sSpider) Parse(crawl *go_scrapy.Engine, resp *go_scrapy.Response) {
	iUrlType := resp.Config[ConfigUrlType]
	urlType := iUrlType.(int)
	logrus.Infof("new quest, url: %s, url type: %d", resp.HttpResponse.Request.URL, urlType)
	httpResp := resp.HttpResponse
	if httpResp.StatusCode != 200 {
		logrus.Error("url %s, StatusCode is %d", httpResp.Request.URL, httpResp.StatusCode)
		return
	}
	switch urlType {
	case UrlTypeMovie:
		spider.ParseMovie(crawl, resp)
	case UrlTypeTeleplay:
		spider.ParseTeleplay(crawl, resp)
	default:
		logrus.Fatal("Url type is err, not is movie or teleplay")
	}
}

func (spider *Movie80sSpider) ParseMovie(crawl *go_scrapy.Engine, resp *go_scrapy.Response) {
	iUrlInfo := resp.Config[ConfigUrlInfo]
	urlInfo := iUrlInfo.(int)
	switch urlInfo {
	case ConfigUrlInfoPage:
		spider.ParseMoviePage(crawl, resp)
	case ConfigUrlInfoDetail:
		spider.ParseMovieDetail(crawl, resp)
	default:
		logrus.Fatal("Url info is err, not is page or detail")
	}
}

func (spider *Movie80sSpider) ParseMoviePage(crawl *go_scrapy.Engine, resp *go_scrapy.Response) {
	httpResp := resp.HttpResponse
	defer httpResp.Body.Close()
	document, err := goquery.NewDocumentFromReader(httpResp.Body)
	if err != nil {
		logrus.Errorf("new document from reader is err: %v", err)
		return
	}
	document.Find(".me1 li>a").Each(func(i int, selection *goquery.Selection) {
		url, exist := selection.Attr("href")
		if !exist {
			return
		}
		title, exist := selection.Attr("title")
		if !exist {
			return
		}
		detailUrl := fmt.Sprintf("%s%s", UrlPre, url)
		httpReq, err := http.NewRequest(http.MethodGet, detailUrl, nil)
		if nil != err {
			logrus.Errorf("New request is err: %v", err)
			return
		}
		crawl.AddRequest(&go_scrapy.Request{
			HttpRequest: httpReq,
			Config: map[string]interface{}{
				ConfigUrlType:    UrlTypeMovie,
				ConfigMovieTitle: title,
				ConfigUrlInfo:    ConfigUrlInfoDetail,
			},
		})
	})
	if spider.PageNumber == 0 {
		document.Find(".pager>a").Each(func(i int, selection *goquery.Selection) {
			pageUrl, _ := selection.Attr("href")
			index := strings.Index(pageUrl, "p")
			if index <= 0 {
				return
			}
			page, err := strconv.ParseUint(pageUrl[index+1:], 10, 64)
			if nil != err {
				logrus.Errorf("convert page string err: %v, pageUrl: %s", err, pageUrl)
				return
			}
			spider.PageNumber = page
		})
		for i := uint64(2); i <= spider.PageNumber; i++ {
			pageUrl := fmt.Sprintf("%s/movie/list/-----p%d", UrlPre, i)
			httpReq, err := http.NewRequest(http.MethodGet, pageUrl, nil)
			if nil != err {
				logrus.Errorf("new request err: %v, url: %s", err, pageUrl)
				continue
			}
			crawl.AddRequest(&go_scrapy.Request{
				HttpRequest: httpReq,
				Config: map[string]interface{}{
					ConfigUrlType: UrlTypeMovie,
					ConfigUrlInfo: ConfigUrlInfoPage,
				},
			})
		}
	}
}

func (spider *Movie80sSpider) ParseMovieDetail(crawl *go_scrapy.Engine, resp *go_scrapy.Response) {
	httpResp := resp.HttpResponse
	defer httpResp.Body.Close()
	document, err := goquery.NewDocumentFromReader(httpResp.Body)
	if err != nil {
		logrus.Errorf("new document from reader is err: %v", err)
		return
	}
	title := resp.Config[ConfigMovieTitle].(string)
	var xunLeiList []string
	document.Find(".xunlei.dlbutton1 a").Each(func(i int, selection *goquery.Selection) {
		xunlei, exist := selection.Attr("href")
		if !exist {
			return
		}
		xunLeiList = append(xunLeiList, xunlei)
	})
	item := &pipelines.VideoItem{
		Title:     title,
		SrcUrl:    httpResp.Request.URL.String(),
		Synopsis:  "",
		OnlineUrl: "",
		LeadActor: "",
		BtUrl:     strings.Join(xunLeiList, "|"),
		Type:      VideoTypeMovie,
	}
	crawl.AddItem(item)
}

func (spider *Movie80sSpider) ParseTeleplay(crawl *go_scrapy.Engine, resp *go_scrapy.Response) {

}
