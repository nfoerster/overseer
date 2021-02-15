package fetcher

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

//HTTP fetcher uses HEAD requests to poll the status of a given
//file. If it detects this file has been updated, it will fetch
//and return its io.Reader stream.
type HTTP struct {
	//URL to poll for new binaries
	URL                 string
	Interval            time.Duration
	CheckHeaders        []string
	InitialHeaderStates map[string]string
	SendHeaderInGet     bool
	Secret              string
	SecretFunc          func() string
	//internal state
	delay bool
	lasts map[string]string
}

//if any of these change, the binary has been updated
var defaultHTTPCheckHeaders = []string{"ETag", "If-Modified-Since", "Last-Modified", "Content-Length"}

// Init validates the provided config
func (h *HTTP) Init() error {
	//apply defaults
	if h.URL == "" {
		return fmt.Errorf("URL required")
	}
	if h.InitialHeaderStates != nil {
		h.lasts = h.InitialHeaderStates
	} else {
		h.lasts = map[string]string{}
	}
	if h.Interval == 0 {
		h.Interval = 5 * time.Minute
	}
	if h.CheckHeaders == nil {
		h.CheckHeaders = defaultHTTPCheckHeaders
	}
	return nil
}

// Fetch the binary from the provided URL
func (h *HTTP) Fetch() (io.Reader, error) {
	//delay fetches after first
	if h.delay {
		time.Sleep(h.Interval)
	}
	h.delay = true

	request, err := http.NewRequest("HEAD", h.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("HEAD request creation failed (%s)", err)
	}
	if len(h.lasts) != 0 {
		for k, v := range h.lasts {
			request.Header.Add(k, v)
		}
	}
	if h.Secret != "" {
		request.Header.Add(h.Secret, h.SecretFunc())
	}
	headresp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("HEAD request failed (%s)", err)
	}
	headresp.Body.Close()
	if headresp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HEAD request failed (status code %d)", headresp.StatusCode)
	}
	//if all headers match, skip update
	matches, total := 0, 0
	for _, header := range h.CheckHeaders {
		if curr := headresp.Header.Get(header); curr != "" {
			if last, ok := h.lasts[header]; ok && last == curr {
				matches++
			}
			//check if headers match, however changing to lasts should happen after GET, otherwise another fetch is not possible
			total++
		}
	}
	if matches == total {
		return nil, nil //skip, file match
	}

	//binary fetch using GET
	var getreq *http.Request
	if h.SendHeaderInGet {
		//use HEAD request, just change method => this will keep the headers from HEAD request
		getreq = request
		getreq.Method = "GET"
	} else {
		getreq, err = http.NewRequest("GET", h.URL, nil)
		if h.Secret != "" {
			getreq.Header.Add(h.Secret, h.SecretFunc())
		}
		if err != nil {
			return nil, fmt.Errorf("GET request creation failed (%s)", err)
		}
	}
	getresp, err := http.DefaultClient.Do(getreq)
	if err != nil {
		return nil, fmt.Errorf("GET request failed (%s)", err)
	}
	if getresp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET request failed (status code %d)", getresp.StatusCode)
	}
	for _, header := range h.CheckHeaders {
		if curr := headresp.Header.Get(header); curr != "" {
			h.lasts[header] = curr
		}
	}
	//extract gz files
	if strings.HasSuffix(h.URL, ".gz") && getresp.Header.Get("Content-Encoding") != "gzip" {
		return gzip.NewReader(getresp.Body)
	}
	//success!
	return getresp.Body, nil
}
