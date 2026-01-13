package electrum

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ClientVersion identifies the client version/name to the remote server
	ClientVersion = "go-electrum1.1"

	// ProtocolVersion identifies the support protocol version to the remote server
	ProtocolVersion = "1.4"

	nl = byte('\n')
)

var (
	// DebugMode provides debug output on communications with the remote server if enabled.
	DebugMode bool

	// ErrServerShutdown throws an error if remote server has shutdown.
	ErrServerShutdown = errors.New("server has shutdown")

	// ErrTimeout throws an error if request has timed out
	ErrTimeout = errors.New("request timeout")
)

// Transport provides interface to server transport.
type Transport interface {
	SendMessage([]byte) error
	Responses() <-chan []byte
	Errors() <-chan error
	Close() error
}

type container struct {
	content []byte
	err     error
}

// Client stores information about the remote server.
type Client struct {
	transport   Transport
	transportMx sync.Mutex

	handlers     map[uint64]chan *container
	handlersLock sync.Mutex

	pushHandlers     map[string][]chan *container
	pushHandlersLock sync.Mutex

	quit     chan struct{}
	quitOnce sync.Once

	nextID uint64
}

// NewClientTCP initialize a new client for remote server and connects to the remote server using TCP
func NewClientTCP(ctx context.Context, addr string, dialTimeout time.Duration) (*Client, error) {
	transport, err := NewTCPTransport(ctx, addr, dialTimeout)
	if err != nil {
		return nil, err
	}

	c := &Client{
		handlers:     make(map[uint64]chan *container),
		pushHandlers: make(map[string][]chan *container),

		quit: make(chan struct{}),
	}

	c.transport = transport
	go c.listen()

	return c, nil
}

// NewClientSSL initialize a new client for remote server and connects to the remote server using SSL
func NewClientSSL(ctx context.Context, addr string, config *tls.Config) (*Client, error) {
	transport, err := NewSSLTransport(ctx, addr, config)
	if err != nil {
		return nil, err
	}

	c := &Client{
		handlers:     make(map[uint64]chan *container),
		pushHandlers: make(map[string][]chan *container),

		quit: make(chan struct{}),
	}

	c.transport = transport
	go c.listen()

	return c, nil
}

type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (A APIError) Error() string {
	return fmt.Sprintf("code: %d, error: %s", A.Code, A.Message)
}

type responseHeader struct {
	ID     uint64    `json:"id"`
	Method string    `json:"method"`
	Error  *APIError `json:"error,omitempty"`
}

func (s *Client) closePushHandlers() {
	s.pushHandlersLock.Lock()
	defer s.pushHandlersLock.Unlock()

	for method, handlers := range s.pushHandlers {
		for _, handler := range handlers {
			select {
			case handler <- &container{err: ErrServerShutdown}:
			default:
			}

			close(handler)
		}

		delete(s.pushHandlers, method)
	}

	s.pushHandlers = nil
}

func (s *Client) closeHandlers() {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()

	for id, c := range s.handlers {
		c <- &container{err: ErrServerShutdown}

		close(c)

		delete(s.handlers, id)
	}

	s.handlers = nil
}

func (s *Client) getTransport() Transport {
	s.transportMx.Lock()
	defer s.transportMx.Unlock()

	return s.transport
}

func (s *Client) parseResponseHeader(bytes []byte) (responseHeader, error) {
	msg := responseHeader{}

	err := json.Unmarshal(bytes, &msg)
	if err != nil {
		return msg, fmt.Errorf("unmarshal received message (%s) failed: %v", string(bytes), err)
	}
	if msg.Error != nil {
		return msg, msg.Error
	}

	return msg, nil
}

func (s *Client) listen() {
	defer s.closePushHandlers()
	defer s.closeHandlers()

	for {
		transport := s.getTransport()
		if transport == nil {
			return
		}

		select {
		case <-s.quit:
			return

		case <-transport.Errors():
			s.Shutdown()

		case bytes := <-transport.Responses():
			result := &container{
				content: bytes,
			}

			hdr, err := s.parseResponseHeader(result.content)
			if err != nil {
				result.err = err
			}

			if len(hdr.Method) > 0 {
				s.pushHandlersLock.Lock()
				handlers := s.pushHandlers[hdr.Method]
				s.pushHandlersLock.Unlock()

				for _, handler := range handlers {
					select {
					case handler <- result:
					default:
					}
				}
			}

			s.handlersLock.Lock()
			if c := s.handlers[hdr.ID]; c != nil {
				c <- result
			}
			s.handlersLock.Unlock()
		}
	}
}

type request struct {
	ID     uint64        `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

func (s *Client) request(ctx context.Context, method string, params []interface{}, v interface{}) error {
	select {
	case <-s.quit:
		return ErrServerShutdown
	default:
	}

	msg := request{
		ID:     atomic.AddUint64(&s.nextID, 1),
		Method: method,
		Params: params,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	bytes = append(bytes, nl)

	transport := s.getTransport()
	if transport == nil {
		return ErrServerShutdown
	}

	if err := transport.SendMessage(bytes); err != nil {
		s.Shutdown()
		return err
	}

	c := make(chan *container, 1)

	s.handlersLock.Lock()
	if s.handlers == nil {
		s.handlersLock.Unlock()
		return ErrServerShutdown
	}
	s.handlers[msg.ID] = c
	s.handlersLock.Unlock()

	defer func() {
		s.handlersLock.Lock()
		if _, ok := s.handlers[msg.ID]; ok {
			close(s.handlers[msg.ID])
			delete(s.handlers, msg.ID)
		}
		s.handlersLock.Unlock()
	}()

	var resp *container
	select {
	case resp = <-c:
	case <-ctx.Done():
		return ErrTimeout
	}
	if resp.err != nil {
		return resp.err
	}

	if v != nil {
		if err := json.Unmarshal(resp.content, v); err != nil {
			return err
		}
	}

	return nil
}

func (s *Client) Shutdown() {
	s.quitOnce.Do(func() {
		close(s.quit)
	})

	s.transportMx.Lock()
	if s.transport != nil {
		_ = s.transport.Close()
	}
	s.transport = nil
	s.transportMx.Unlock()
}
