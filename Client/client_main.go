package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/smallnest/goframe"
)

type msg struct {
	Topic   string
	Body    string
	Version int32
	Time    int64
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9001")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	encoderConfig := goframe.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}

	decoderConfig := goframe.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}

	fc := goframe.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, conn)

	m := &msg{}
	m.Topic = "testKfk"
	m.Version = 1
	m.Time = time.Now().Unix()
	m.Body = "#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXx12131231231231__"

	bufMsg, _ := json.Marshal(m)
	err = fc.WriteFrame(bufMsg)
	if err != nil {
		panic(err)
	}

	buf, err := fc.ReadFrame()
	if err != nil {
		panic(err)
	}
	fmt.Println("received: ", string(buf))

}
