package sordini

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Broker is the interface that wraps the Broker's methods.
type Handler interface {
	Run(context.Context, <-chan *Context, chan<- *Context)
	RegisterCallback(Callback)
	Leave() error
	Shutdown() error
}

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	addr         string
	protocolLn   *net.TCPListener
	handler      Handler
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	requestCh    chan *Context
	responseCh   chan *Context
}

func NewServer(addr string) (*Server, error) {
	b, err := NewBroker(addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		addr:       addr,
		handler:    b,
		shutdownCh: make(chan struct{}),
		requestCh:  make(chan *Context, 1024),
		responseCh: make(chan *Context, 1024),
	}
	return s, nil
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf(protocolAddr.String())
	if s.protocolLn, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			default:
				conn, err := s.protocolLn.Accept()
				if err != nil {
					log.Errorf("server: listener accept error: %s", err)
					continue
				}

				go s.handleRequest(conn)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			case respCtx := <-s.responseCh:

				if err := s.handleResponse(respCtx); err != nil {
					log.Errorf("server: handle response error: %s", err)
				}
			}
		}
	}()

	log.Debug("server: run handler")
	go s.handler.Run(ctx, s.requestCh, s.responseCh)

	return nil
}

func (s *Server) RegisterCallback(cb Callback) {
	s.handler.RegisterCallback(cb)
}

func (s *Server) Leave() error {
	return s.handler.Leave()
}

// Shutdown closes the service.
func (s *Server) Shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	close(s.shutdownCh)

	if err := s.handler.Shutdown(); err != nil {
		return err
	}
	if err := s.protocolLn.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		p := make([]byte, 4)
		_, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("conn read error: %s", err)
			break
		}

		size := binary.BigEndian.Uint32(p)
		if size == 0 {
			break // TODO: should this even happen?
		}

		b := make([]byte, size) //+4 since we're going to copy the size into b
		// copy(b, p)

		if _, err = io.ReadFull(conn, b); err != nil {
			// TODO: handle request
			panic(err)
		}

		d := &kbin.Reader{
			Src: b,
		}

		key := kmsg.Key(d.Int16())
		version := d.Int16()
		correlationId := d.Int32()
		clientId := d.NullableString()

		req := key.Request()
		req.SetVersion(version)

		if err := req.ReadFrom(d.Src); err != nil {
			log.Errorf("server: %s: decode request failed: %s", key.Name(), err)
			panic(err)
		}

		reqCtx := &Context{
			correlationID: correlationId,
			clientID:      clientId,
			req:           req,
			conn:          conn,
		}

		log.Debugf("server: handle request: %v", reqCtx)

		s.requestCh <- reqCtx
	}
}

func (s *Server) handleResponse(respCtx *Context) error {
	log.Debugf("server: handle response: %v", respCtx)

	b, err := respCtx.ResponseToBytes()
	if err != nil {
		return err
	}

	_, err = respCtx.conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.protocolLn.Addr()
}
