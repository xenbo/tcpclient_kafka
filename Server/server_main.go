package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/xenbo/go_kfk_client/rdkfk"
	"log"
	"time"

	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pool/goroutine"
)

type msg struct {
	Topic   string
	Body    string
	Version int32
	Time    int64
}
type KRouterServer struct {
	*gnet.EventServer
	addr       string
	multicore  bool
	async      bool
	codec      gnet.ICodec
	workerPool *goroutine.Pool
	topics     map[string]bool
	kc         rdkfk.KafkaClient
}

func (cs *KRouterServer) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	log.Printf("Test codec server is listening on %s (multi-cores: %t, loops: %d)\n",
		srv.Addr.String(), srv.Multicore, srv.NumLoops)
	return
}

func (cs *KRouterServer) React(c gnet.Conn) (out []byte, action gnet.Action) {
	if cs.async {
		data := append([]byte{}, c.ReadFrame()...)

		if len(data) > 0 {
			_ = cs.workerPool.Submit(func() {
				m := &msg{}
				json.Unmarshal(data, m)
				_, ok := cs.topics[m.Topic]
				if !ok && m.Topic != "" {
					cs.kc.AddProduceTopic(m.Topic)
					cs.topics[m.Topic] = true
				}
				if m.Topic != "" {
					cs.kc.SendMsgWithCache(m.Topic, string(data))
				}
				c.AsyncWrite(data)
			})
		}

	} else {
		out = c.ReadFrame()
		if len(out) > 0 {
			m := &msg{}
			json.Unmarshal(out, m)
			_, ok := cs.topics[m.Topic]
			if !ok && m.Topic != "" {
				cs.kc.AddProduceTopic(m.Topic)
				cs.topics[m.Topic] = true
			}
			if m.Topic != "" {
				cs.kc.SendMsgWithCache(m.Topic, string(out))
			}
		}
	}
	return
}

func (cs *KRouterServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	fmt.Println("fuck close", cs.topics)

	return
}

func SvrRun(addr string, multicore, async bool, codec gnet.ICodec) {
	var err error
	if codec == nil {
		encoderConfig := gnet.EncoderConfig{
			ByteOrder:                       binary.BigEndian,
			LengthFieldLength:               4,
			LengthAdjustment:                0,
			LengthIncludesLengthFieldLength: false,
		}
		decoderConfig := gnet.DecoderConfig{
			ByteOrder:           binary.BigEndian,
			LengthFieldOffset:   0,
			LengthFieldLength:   4,
			LengthAdjustment:    0,
			InitialBytesToStrip: 4,
		}
		codec = gnet.NewLengthFieldBasedFrameCodec(encoderConfig, decoderConfig)
	}
	cs := &KRouterServer{addr: addr, multicore: multicore, async: async, codec: codec, workerPool: goroutine.Default()}
	cs.kc.NewProducer()
	cs.topics = make(map[string]bool)

	err = gnet.Serve(cs, addr, gnet.WithMulticore(multicore), gnet.WithTCPKeepAlive(time.Minute*5), gnet.WithCodec(codec), gnet.WithReusePort(true))
	if err != nil {
		panic(err)
	}
}

func main() {
	glc := rdkfk.GlobeCleaner{}
	glc.SetKafkaAddr("192.168.1.172")
	glc.Init()

	var port int
	var multicore bool

	// Example command: go run server.go --port 9000 --multicore true
	flag.IntVar(&port, "port", 9001, "server port")
	flag.BoolVar(&multicore, "multicore", true, "multicore")
	flag.Parse()

	addr := fmt.Sprintf("tcp://:%d", port)
	SvrRun(addr, true, true, nil)
}
