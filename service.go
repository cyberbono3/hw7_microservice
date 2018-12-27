package main

import (
	context "context"
	"encoding/json"

	"log"
	"net"
	"strings"
	"time"
	
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
type AdminHandler struct {
	ctx context.Context

	broadcastLogCh   chan *Event
	addLogListenerCh chan chan *Event
	logListeners     []chan *Event

	broadcastStatCh   chan *Stat
	addStatListenerCh chan chan *Stat
	statListeners     []chan *Stat
}

type BizHandler struct {
}

type ACL map[string][]string

type Server struct {
	acl ACL
	AdminHandler
	BizHandler
}

func StartMyMicroservice(ctx context.Context, addr, acl string) error {
	srv := &Server{}
	srv.ctx = ctx

	if err := json.Unmarshal([]byte(acl), &srv.acl); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(srv.streamInterceptor),
		grpc.UnaryInterceptor(srv.unaryInterceptor),
	)
	RegisterBizServer(grpcServer, srv)
	RegisterAdminServer(grpcServer, srv)

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	srv.broadcastLogCh = make(chan *Event, 0)
	srv.addLogListenerCh = make(chan chan *Event, 0)
	go func() {
		for {
			select {
			case ch := <-srv.addLogListenerCh:
				srv.logListeners = append(srv.logListeners, ch)
			case event := <-srv.broadcastLogCh:
				for _, ch := range srv.logListeners {
					ch <- event
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	srv.broadcastStatCh = make(chan *Stat, 0)
	srv.addStatListenerCh = make(chan chan *Stat, 0)
	go func() {
		for {
			select {
			case ch := <-srv.addStatListenerCh:
				srv.statListeners = append(srv.statListeners, ch)
			case stat := <-srv.broadcastStatCh:
				for _, ch := range srv.statListeners {
					ch <- stat
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (b *BizHandler) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BizHandler) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (b *BizHandler) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (s *Server) checks(ctx context.Context, fullMethod string) error {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	consumer, ok := meta["consumer"]
	if !ok || len(consumer) != 1 {
		return grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	allowedPaths, ok := s.acl[consumer[0]]
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "NO! means NO!")
	}

	splittedMethod := strings.Split(fullMethod, "/")
	if len(splittedMethod) != 3 {
		return grpc.Errorf(codes.Unauthenticated, "NO! means NO!")
	}

	path, method := splittedMethod[1], splittedMethod[2]
	isAllowed := false
	for _, al := range allowedPaths {
		splitted := strings.Split(al, "/")
		if len(splitted) != 3 {
			continue
		}
		allowedPath, allowedMethod := splitted[1], splitted[2]
		if path != allowedPath {
			continue
		}
		if allowedMethod == "*" || method == allowedMethod {
			isAllowed = true
			break
		}
	}
	if !isAllowed {
		return grpc.Errorf(codes.Unauthenticated, "NO! means NO!")
	}
	return nil
}

func (s *Server) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := s.checks(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	consumer, ok := meta["consumer"]
	if !ok || len(consumer) != 1 {
		return nil, grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	s.broadcastLogCh <- &Event{
		Consumer: consumer[0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}
	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{consumer[0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}
	return handler(ctx, req)
}

func (s *Server) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.checks(ss.Context(), info.FullMethod); err != nil {
		return err
	}

	meta, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	consumer, ok := meta["consumer"]
	if !ok || len(consumer) != 1 {
		return grpc.Errorf(codes.Unauthenticated, "can't get metadata")
	}

	s.broadcastLogCh <- &Event{
		Consumer: consumer[0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}
	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{consumer[0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}
	return handler(srv, ss)
}

func (s *AdminHandler) Logging(nothing *Nothing, srv Admin_LoggingServer) error {
	ch := make(chan *Event, 0)
	s.addLogListenerCh <- ch

	for {
		select {
		case event := <-ch:
			srv.Send(event)
		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
}

func (s *AdminHandler) Statistics(interval *StatInterval, srv Admin_StatisticsServer) error {
	ch := make(chan *Stat, 0)
	s.addStatListenerCh <- ch

	ticker := time.NewTicker(time.Second * time.Duration(interval.IntervalSeconds))
	sum := &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}
	for {
		select {
		case stat := <-ch:
			for k, v := range stat.ByMethod {
				sum.ByMethod[k] += v
			}
			for k, v := range stat.ByConsumer {
				sum.ByConsumer[k] += v
			}

		case <-ticker.C:
			srv.Send(sum)
			sum = &Stat{
				ByMethod:   make(map[string]uint64),
				ByConsumer: make(map[string]uint64),
			}

		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
}
