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

	"github.com/artyom/autoflags"
	"github.com/mediocregopher/radix.v2/redis"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func main() {
	params := struct {
		RedisAddr string `flag:"b,bitmapist address"`
		Addr      string `flag:"l,udp address to listen at"`
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
	p := newPusher(params.RedisAddr)
	log.Fatal(p.handleUDP(params.Addr))
}

func newPusher(addr string) *pusher {
	p := &pusher{
		q: make(chan payload, 1000),
	}
	go p.process(addr)
	return p
}

type pusher struct {
	q chan payload
}

func (p *pusher) process(addr string) {
	var cl *redis.Client
	var err error
	var out []payload
	ticker := time.NewTicker(5 * time.Second)

reconnect:
	for {
		cl, err = redis.DialTimeout("tcp", addr, 15*time.Second)
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
			if len(out) == 0 {
				continue
			}
			for _, v := range out {
				sid := strconv.Itoa(int(v.UID))
				for _, evt := range v.Events {
					for _, f := range []keyFunc{mKey, wKey, dKey} {
						cl.PipeAppend("SETBIT", f(evt, now), sid, "1")
					}
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
			maxcap := cap(out)
			if maxcap > 500 {
				maxcap = 500
			}
			out = out[:0:maxcap]
		case msg := <-p.q:
			out = append(out, msg)
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
				return
			}
			select {
			case p.q <- data:
			default:
			}
		}(p, n, bufp)
	}
}

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
