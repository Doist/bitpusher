// Command bitpusher forwards udp packets with userid/events to bitmapist instance
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/artyom/autoflags"
	"github.com/mediocregopher/radix.v2/redis"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// version is attached to every metric as a `version:` tag
// https://handbook.doist.com/doc/standard-observability-vWRaOfitho
// It is injected at build time via -ldflags "-X main.version=..." (see the
// Dockerfile); a build that omits it falls back to "dev"
var version = "dev"

func main() {
	params := struct {
		RedisAddr  string `flag:"b,bitmapist address"`
		Addr       string `flag:"l,udp address to listen at"`
		StatsdAddr string `flag:"statsd,dogstatsd address for metrics (empty disables)"`
		Project    string `flag:"project,product this deployment serves, added as a project:<value> metric tag, e.g. todoist, comms, automations"`
	}{
		RedisAddr: "localhost:6379",
		Addr:      "localhost:25800",
	}
	autoflags.Define(&params)
	flag.Parse()
	if params.RedisAddr == "" || params.Addr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Metrics are optional. With no -statsd address we use a no-op client so the
	// emit sites stay unconditional (no nil checks) and never touch the network.
	var m statsd.ClientInterface = &statsd.NoOpClient{}
	if params.StatsdAddr != "" {
		tags := []string{"service:bitpusher", "version:" + version}
		if params.Project != "" {
			tags = append(tags, "project:"+params.Project)
		}
		c, err := statsd.New(params.StatsdAddr,
			statsd.WithNamespace("bitpusher"), // prefixes every metric name
			statsd.WithTags(tags),             // attached to every metric
			statsd.WithChannelMode(),          // drop metrics rather than block if the buffer fills
		)

		if err != nil {
			log.Printf("metrics disabled: %v", err)
		} else {
			m = c
		}
	}

	log.Printf("bitpusher version=%s listening on %s -> bitmapist %s (statsd=%q project=%q)",
		version, params.Addr, params.RedisAddr, params.StatsdAddr, params.Project)

	p := newPusher(params.RedisAddr, m)
	err := p.handleUDP(params.Addr)
	m.Close() // flush buffered metrics before exiting
	log.Fatal(err)
}

func newPusher(addr string, m statsd.ClientInterface) *pusher {
	p := &pusher{
		q: make(chan event, 10000),
		m: m,
	}
	go p.process(addr)
	return p
}

type pusher struct {
	q chan event
	m statsd.ClientInterface
}

func (p *pusher) process(addr string) {
	var cl *redis.Client
	var err error
	out := make(map[event]struct{})
	ticker := time.NewTicker(5 * time.Second)

reconnect:
	for {
		cl, err = redisDial(addr)
		if err != nil {
			log.Print(err)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	for {
		select {
		case now := <-ticker.C:
			p.m.Gauge("queue_depth", float64(len(p.q)), nil, 1)
			if len(out) == 0 {
				continue
			}
			for v := range out {
				sid := strconv.Itoa(int(v.uid))
				for _, f := range []keyFunc{mKey, wKey, dKey} {
					cl.PipeAppend("SETBIT", f(v.evt, now), sid, "1")
				}
			}
		drainResponse:
			for {
				r := cl.PipeResp()
				switch r.Err {
				case nil:
					continue
				case redis.ErrPipelineEmpty:
					break drainResponse
				}
				if r.IsType(redis.IOErr) {
					log.Print(r.Err)
					cl.Close()
					goto reconnect
				}
			}

			// Count only after a successful drain.
			p.m.Count("events_flushed", int64(len(out)), nil, 1)
			out = make(map[event]struct{})
		case evt := <-p.q:
			out[evt] = struct{}{}
		}
	}
}

func (p *pusher) handleUDP(addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	for {
		bufp := bufPool.Get().(*[]byte)
		n, _, err := conn.ReadFrom(*bufp)
		if err != nil {
			return err
		}
		go func(p *pusher, n int, bufp *[]byte) {
			defer bufPool.Put(bufp)
			var data payload
			if msgpack.Unmarshal((*bufp)[:n], &data) != nil {
				p.m.Incr("decode_errors", nil, 1)
				return
			}
			p.m.Count("events_received", int64(len(data.Events)), nil, 1)
			for _, e := range data.Events {
				evt := event{data.UID, e}
				select {
				case p.q <- evt:
				default:
					p.m.Incr("events_dropped", nil, 1)
				}
			}
		}(p, n, bufp)
	}
}

// event presents a single user event
type event struct {
	uid uint32
	evt string
}

// payload is a message received by UDP listener, may hold multiple events of a single user
type payload struct {
	UID    uint32   `msgpack:"id"`
	Events []string `msgpack:"ev"`
}

type keyFunc func(string, time.Time) string

func dKey(k string, t time.Time) string { return "trackist_" + k + "_" + t.Format("2006-1-2") }
func mKey(k string, t time.Time) string { return "trackist_" + k + "_" + t.Format("2006-1") }
func wKey(k string, t time.Time) string {
	y, w := t.ISOWeek()
	return "trackist_" + k + "_W" + strconv.Itoa(y) + "-" + strconv.Itoa(w)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4096)
		return &buf
	},
}

func redisDial(addr string) (*redis.Client, error) {
	const timeout = 15 * time.Second
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(time.Minute)
	}
	cl, err := redis.NewClient(conn)
	if err != nil {
		return nil, err
	}
	cl.ReadTimeout = timeout
	cl.WriteTimeout = timeout
	return cl, nil
}
