package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"

	// "github.com/andrewchambers/extraio"
	"github.com/andrewchambers/foreverconn"
)

func main() {
	log.SetOutput(os.Stderr)

	server := flag.Bool("server", false, "Act as a server")
	flag.Parse()

	if *server {
		log.Printf("server mode")
		l, err := net.Listen("tcp", "127.0.0.1:3000")
		if err != nil {
			log.Fatal(err)
		}
		defer l.Close()
		log.Printf("listening on %s", l.Addr())

		firstConn, err := l.Accept()
		if err != nil {
			log.Printf("initial persistent connection failed: %s", err)
		}

		isFirst := new(bool)
		*isFirst = true
		reconnect := func() (io.ReadWriteCloser, error) {
			log.Printf("transient connection (re)connecting")
			if *isFirst {
				*isFirst = false
				return firstConn, nil
			}
			conn, err := l.Accept()
			return conn, err
		}

		log.Printf("starting persistent connection")
		persistent, err := net.Dial("tcp", "127.0.0.1:22")
		if err != nil {
			log.Print("initial persistent connection failed: %s", err)
		} else {
			err = foreverconn.Proxy(persistent, reconnect)
			if err != nil {
				log.Printf("unrecoverable persistent connection error: %s", err)
			}
		}
	} else {
		log.Printf("client mode")
		l, err := net.Listen("tcp", "127.0.0.1:2000")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("listening on %s for one persistent connection", l.Addr())
		persistent, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		_ = l.Close()
		log.Printf("persistent connection arrived")

		reconnect := func() (io.ReadWriteCloser, error) {
			log.Printf("transient connection (re)connecting")
			conn, err := net.Dial("tcp", "127.0.0.1:3000")
			return conn, err
		}

		err = foreverconn.Proxy(persistent, reconnect)
		if err != nil {
			log.Printf("unrecoverable persistent connection error: %s", err)
		}
	}

}
