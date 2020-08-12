package server_test

import (
	"sync"
	"testing"
	"time"

	rpcproto "github.com/hatobito-io/natsrpc/proto"
	"github.com/hatobito-io/natsrpc/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	nserver "github.com/nats-io/nats-server/v2/server"
)

type resultStruct struct {
	reply    *rpcproto.Sample
	replyErr *rpcproto.Error
	err      error
}
type Service struct{}

var _ server.Service = &Service{}

func (s *Service) Subject() string {
	return "sampleService"
}

func (s *Service) Handle(req *server.Request) (proto.Message, error) {
	data := req.Message.Data
	var input rpcproto.Error
	if err := proto.Unmarshal(data, &input); err != nil {
		return nil, nil
	}
	switch req.Method {
	case "ask":
		return &rpcproto.Sample{Message: "ANSWER to " + input.Message}, nil
	case "slow":
		<-time.After(time.Millisecond * 100)
		return &rpcproto.Sample{Message: "ANSWER"}, nil
	default:
		return &rpcproto.Error{Message: "NO_METHOD"}, nil
	}
}

var _ = Describe("Server", func() {
	var conn *nats.Conn
	var srv server.Server
	var clientConn *nats.Conn
	var service *Service
	BeforeEach(func() {
		var err error
		conn, err = nats.Connect(natsURL)
		Expect(err).To(Succeed())
		service = &Service{}
		srv, err = server.Start(conn, service, server.WithPoolSize(500), server.WithMaxWaiting(5000))
		Expect(err).To(Succeed())
		clientConn, err = nats.Connect(natsURL)
		Expect(err).To(Succeed())
	})
	AfterEach(func() {
		clientConn.Close()
		srv.Stop()
		conn.Close()
	})

	It("can serve a request", func() {
		q := time.Now().String()
		req := &rpcproto.Error{Message: q}
		reply, replyErr, err := sendRequest(clientConn, service.Subject(), "ask", req, time.Second)
		Expect(err).To(Succeed())
		Expect(replyErr).To(BeNil())
		Expect(reply.Message).To(Equal("ANSWER to " + q))
	})

	It("returns expected error from unknown method request", func() {
		req := &rpcproto.Error{Message: "QUESTION"}
		reply, replyErr, err := sendRequest(clientConn, service.Subject(), "wtf", req, time.Second)
		Expect(err).To(Succeed())
		Expect(replyErr.Message).To(Equal("NO_METHOD"))
		Expect(reply).To(BeNil())
	})

	It("returns expected error if server is too busy", func() {
		req := &rpcproto.Error{Message: "QUESTION"}

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()
			sendRequest(clientConn, service.Subject(), "slow", req, time.Millisecond*200)
		}()
		reply, replyErr, err := sendRequest(clientConn, service.Subject(), "slow", req, time.Millisecond*200)
		Expect(err).To(Succeed())
		Expect(replyErr).To(BeNil())
		Expect(reply).ToNot(BeNil())
		Expect(reply.Message).To(Equal("ANSWER"))

		srv.GetPool().SetMaxWaiting(0)
		srv.GetPool().SetSize(1)
		srv.SetTimeout(time.Millisecond * 75)

		wg.Add(1)
		go func() {
			defer wg.Done()
			sendRequest(clientConn, service.Subject(), "slow", req, time.Millisecond*200)
		}()
		<-time.After(time.Millisecond * 20)
		reply, replyErr, err = sendRequest(clientConn, service.Subject(), "slow", req, time.Millisecond*200)
		Expect(err).To(Succeed())
		Expect(reply).To(BeNil())
		Expect(replyErr).ToNot(BeNil())
		Expect(replyErr.Code).To(Equal(rpcproto.ErrorCode_TOOBUSY))

	})

	It("handles some load", func() {
		numRequests := 5000
		var wg sync.WaitGroup
		ch := make(chan resultStruct, numRequests)
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			<-time.After(time.Microsecond * 5)
			go func() {
				defer wg.Done()
				q := time.Now().String()
				req := &rpcproto.Error{Message: q}
				reply, replyErr, err := sendRequest(clientConn, service.Subject(), "ask", req, time.Second)
				ch <- resultStruct{
					reply:    reply,
					replyErr: replyErr,
					err:      err,
				}
			}()
		}
		wg.Wait()
		close(ch)
		success := 0
		fail := 0
		for r := range ch {
			if r.err != nil || r.replyErr != nil {
				fail++
			} else {
				success++
			}
		}
		Expect(success).To(Equal(numRequests))
		Expect(fail).To(Equal(0))
	})

})

func sendRequest(conn *nats.Conn, subject string, method string, request proto.Message, timeout time.Duration) (*rpcproto.Sample, *rpcproto.Error, error) {
	bytes, err := proto.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	msg, err := conn.Request(subject+"."+method, bytes, timeout)
	if err != nil {
		return nil, nil, err
	}
	var resp rpcproto.Response
	err = proto.Unmarshal(msg.Data, &resp)
	if err != nil {
		return nil, nil, err
	}
	if e := resp.GetError(); e != nil {
		return nil, e, nil
	}
	res := resp.GetResult()
	var sample rpcproto.Sample
	err = proto.Unmarshal(res.Value, &sample)
	if err != nil {
		return nil, nil, err
	}
	return &sample, nil, nil
}

func startNATS() (*nserver.Server, error) {
	opts := &nserver.Options{
		Port: nserver.RANDOM_PORT,
	}
	return nserver.NewServer(opts)
}

var natsURL string

func TestServer(t *testing.T) {
	srv, err := startNATS()
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(time.Millisecond * 1000) {
		t.Fatal("NATS server failed to start")
	}
	natsURL = srv.ClientURL()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Server Suite")
	srv.Shutdown()
}
