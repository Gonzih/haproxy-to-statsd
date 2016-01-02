package main

import (
	"testing"
)

func TestBasicParsing(t *testing.T) {
	var sampleString = `Nov 28 18:28:58 localhost haproxy[19044]: 127.0.0.1:40598 [28/Nov/2015:18:28:58.483] http-in http-in/cluster-1 0/0/0/11/11 200 402 - - ---- 0/0/0/0/0 0/0 \"POST /solr/blabla/select HTTP/1.0\"`

	data := RegexpSubmatchesToMap(haProxyLogRe, sampleString)

	if data["clientPort"] != "40598" ||
		data["clientIp"] != "127.0.0.1" {
		t.Fail()
	}
}
