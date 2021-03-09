package boltc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
)

func makeURL(baseURL, resUri string) string {
	u, _ := url.Parse(baseURL)
	u.Path = path.Join(u.Path, resUri)
	return u.String()
}

func doPostJSON(url string, req interface{}, respObj interface{}) (err error) {
	d, err := json.Marshal(req)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Post(url, "application/json", bytes.NewReader(d))
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("response %d %s", resp.StatusCode, resp.Status)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	d, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(d, respObj)
	return
}
