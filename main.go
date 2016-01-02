package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"time"
)

const (
	haProxyTsLayout = "2/Jan/2006:15:04:05.000"
)

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

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
		// this line breaks example
		// `"(?P<request>(?P<requestMethod>[^ ]+) (?P<requestPath>.*)(?: HTTP\/[0-9\.]+))"?` +
		`|` +
		`(?P<error>[a-zA-Z0-9: ]+)` +
		`)`,
	)
)

var sampleString = `Nov 28 18:28:58 localhost haproxy[19044]: 127.0.0.1:40598 [28/Nov/2015:18:28:58.483] http-in http-in/cluster-1 0/0/0/11/11 200 402 - - ---- 0/0/0/0/0 0/0 \"POST /solr/blabla/select HTTP/1.0\"`

func RegexpSubmatchesToMap(re *regexp.Regexp, input string) map[string]string {
	submatches := re.FindStringSubmatch(input)

	if submatches == nil {
		fmt.Fprintf(os.Stderr, "Error while parsing payload: \"%s\"\n", input)
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

	skip := true

	file, err := os.Open(filePath)

	defer file.Close()

	CheckErr(err)

	reader := bufio.NewReader(file)

	for {
		part, _, err := reader.ReadLine()

		if err != io.EOF {
			if !skip {
				channel <- string(part)
			}
		} else {
			if !skip {
				time.Sleep(10 * time.Millisecond)
			} else {
				skip = false
			}
		}
	}

}

type Entry struct {
	key  string
	time string
}

func MapToEntry(data map[string]string) Entry {
	hostname, _ := os.Hostname()
	key := fmt.Sprintf("haproxy.%s.%s", hostname, data["server"])
	time := data["tt"]

	return Entry{
		key:  key,
		time: time}
}

func EntryToStatsdStrings(entry Entry) (results [2]string) {
	// haproxy.er-web1.backend:30|ms|@0.1
	// haproxy.er-web1.backend:1|c|@0.1
	// haproxy.er-web1.backend:30|ms
	// haproxy.er-web1.backend:1|c

	results[0] = fmt.Sprintf("%s:%s|ms", entry.key, entry.time)
	results[1] = fmt.Sprintf("%s:1|c", entry.key)

	return
}

func Process(channel chan string) {
	localAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	CheckErr(err)

	remoteAddr, err := net.ResolveUDPAddr("udp", "localhost:8125")
	CheckErr(err)

	conn, err := net.DialUDP("udp", localAddr, remoteAddr)
	CheckErr(err)

	defer conn.Close()

	for {
		payload := <-channel
		data := RegexpSubmatchesToMap(haProxyLogRe, payload)

		if data != nil {
			entry := MapToEntry(data)
			dataStrings := EntryToStatsdStrings(entry)

			for _, dataString := range dataStrings {
				buf := []byte(dataString)
				conn.Write(buf)
			}
		}
	}
}

func main() {
	flag.Parse()

	for _, filePath := range flag.Args() {
		followChannel := make(chan string, 100)
		go Follow(filePath, followChannel)
		go Process(followChannel)
	}

	if len(flag.Args()) > 0 {
		done := make(chan bool)
		<-done
	}
}
