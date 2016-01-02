package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"
)

const (
	haProxyTsLayout = "2/Jan/2006:15:04:05.000"
)

var (
	// NB: See section 8.2.3 of http://haproxy.1wt.eu/download/1.5/doc/configuration.txt to see HAProxy log format details.
	// Example line:
	//     <142>Sep 27 00:15:57 haproxy[28513]: 67.188.214.167:64531 [27/Sep/2013:00:15:43.494] frontend~ test/10.127.57.177-10000 449/0/0/13531/13980 200 13824 - - ---- 6/6/0/1/0 0/0 "GET / HTTP/1.1"
	haProxyLogRe = regexp.MustCompile(`^` +
		// `<(?P<logLevel>\d+)>` +
		`[a-zA-Z0-9: ]+ ` +
		`(?P<processName>[a-zA-Z_\.-]+)\[(?P<pid>[^\]]+)\]: ` +
		`(?P<clientIp>[0-9\.]+):(?P<clientPort>[0-9]+) ` +
		`\[(?P<acceptTs>[^\]]+)\] ` +
		`(?P<frontend>[^ ]+) ` +
		`(?:` +
		`(?P<backend>[^\/]+)\/(?P<server>[^ ]+) ` +
		`(?P<tq>-?\d+)\/(?P<tw>-?\d+)\/(?P<tc>-?\d+)\/(?P<tr>-?\d+)\/(?P<tt>\+?\d+) ` +
		`(?P<statusCode>\d+) ` +
		`(?P<bytesRead>\d+) ` +
		`(?P<terminationState>[a-zA-Z -]+) ` +
		`(?P<actConn>\d+)\/(?P<feConn>\d+)\/(?P<beConn>\d+)\/(?P<srvConn>\d+)\/(?P<retries>\+?\d) ` +
		`(?P<srvQueue>\d+)\/(?P<backendQueue>\d+) ` +
		`(?P<headers>\{.*\} *)*` +
		`"(?P<request>(?P<requestMethod>[^ ]+) (?P<requestPath>.*)(?: HTTP\/[0-9\.]+))"?` +
		`|` +
		`(?P<error>[a-zA-Z0-9: ]+)` +
		`)`,
	)
)

var sampleString = `Nov 28 18:28:58 localhost haproxy[19044]: 127.0.0.1:40598 [28/Nov/2015:18:28:58.483] http-in http-in/cluster-1 0/0/0/11/11 200 402 - - ---- 0/0/0/0/0 0/0 \"POST /solr/blabla/select HTTP/1.0\"`

func RegexpSubmatchesToMap(re *regexp.Regexp, input string) map[string]string {
	submatches := re.FindStringSubmatch(input)

	if submatches == nil {
		return nil
	}

	data := map[string]string{}

	for i, key := range haProxyLogRe.SubexpNames() {
		data[key] = string(submatches[i])
		//fmt.Printf("data[%v] = \"%v\"\n", key, data[key])
	}

	return data
}

func Follow(filePath string, channel chan string) {
	fmt.Println("Monitoring", filePath)

	file, err := os.Open(filePath)

	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(file)

	for {
		part, _, err := reader.ReadLine()

		if err != io.EOF {
			channel <- string(part)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}

}

func Process(channel chan string) {
	payload := <-channel
}

func main() {
	flag.Parse()

	for _, filePath := range flag.Args() {
		followChannel := make(chan string, 100)
		go Follow(filePath, followChannel)
		go Process(followChannel)
	}

	fmt.Println(RegexpSubmatchesToMap(haProxyLogRe, sampleString))
}
