package main

import (
	context "context"
	"encoding/json"
	"net"
	"log"


	grpc "google.golang.org/grpc"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

//structura servisa

type AdminHandler struct {
	ctx context.Context
	broadcastLogCh chan *Event
	addLogListenerCh chan chan *Event
	logListeners []chan *Event

	broadcastStatCh chan *Stat
	addStatListenerCh chan chan *Event
	statListeners []chan *Stat

}

type BizHandler struct {

}

type ACL map[string][]string

type Server struct{
	acl ACL
	AdminHandler
	BizHandler
}


func StartMyMicroService(ctx context.Context, addr, acl string) error {

	srv := new(Server)
	srv.ctx = ctx

	//transform acl json string to srv.acl
	if err := json.Unmarshal([]byte(acl), &srv.acl); err!=nil {
		return err
	}

	lis, err :=  net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(srv.streamInterceptor),
		grpc.UnaryInterceptor(srv.unaryInterceptor),
	)

	RegisterBizServer(grpcServer, srv)
	RegisterAdminServer(grpcServer, srv)

	//run server listener in go routine
	go func() {
		if err := grpcServer.Serve(lis); err!=nil {
			log.Fatal(err)
		}

	}()
	
	//stop the server if ctx is done
	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	srv.broadcastLogCh = make(chan *Event, 0 )
	srv.addLogListenerCh = make (chan chan *Event, 0 )

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

	srv.broadcastStatCh = make(chan *Stat, 0 )
	srv.addLogListenerCh = make (chan chan *Stat, 0 )

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






	








}












//funkcija kotoraja vozvraschaet strukturu servisa




//metodi servisa




