package operation

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ufilesdk-dev/us3-qiniu-go-sdk/api.v7/auth/qbox"
)

var downloadClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 10 * time.Minute,
}

type Downloader struct {
	bucket      string
	ioHosts     []string
	credentials *qbox.Mac
	queryer     *Queryer
}

func NewDownloader(c *Config) *Downloader {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	downloader := Downloader{
		bucket:      c.Bucket,
		ioHosts:     dupStrings(c.IoHosts),
		credentials: mac,
		queryer:     queryer,
	}
	shuffleHosts(downloader.ioHosts)
	return &downloader
}
func NewDownloaderV2() *Downloader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewDownloader(c)
}

func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path)
		if err == nil {
			return
		}
	}
	return
}

func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key)
		if err == nil {
			break
		}
	}
	return
}

func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	for i := 0; i < 3; i++ {
		l, data, err = d.downloadRangeBytesInner(key, offset, size)
		if err == nil {
			break
		}
	}
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

var curIoHostIndex uint32 = 0

func (d *Downloader) nextHost() string {
	ioHosts := d.ioHosts
	if d.queryer != nil {
		if hosts := d.queryer.QueryIoHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			ioHosts = hosts
		}
	}
	switch len(ioHosts) {
	case 0:
		panic("No Io hosts is configured")
	case 1:
		return ioHosts[0]
	default:
		var ioHost string
		for i := 0; i <= len(ioHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curIoHostIndex, 1) - 1)
			ioHost = ioHosts[index%len(ioHosts)]
			if isHostNameValid(ioHost) {
				break
			}
		}
		return ioHost
	}
}

func (d *Downloader) downloadFileInner(key, path string) (*os.File, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	host := d.nextHost()

	fmt.Println("remote path", key)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		succeedHostName(host)
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *Downloader) downloadBytesInner(key string) ([]byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	host := d.nextHost()

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	return ioutil.ReadAll(response.Body)
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size)
}

func (d *Downloader) downloadRangeBytesInner(key string, offset, size int64) (int64, []byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	host := d.nextHost()

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}

	req.Header.Set("Range", generateRange(offset, size))
	response, err := downloadClient.Do(req)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		failHostName(host)
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		failHostName(host)
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		failHostName(host)
		return -1, nil, err
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		failHostName(host)
	} else {
		succeedHostName(host)
	}
	return l, b, err
}

func getTotalLength(crange string) (int64, error) {
	cr := strings.Split(crange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + crange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}
