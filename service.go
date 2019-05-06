/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/untillpro/airs-iconfig"
	"github.com/untillpro/gochips"
	"golang.org/x/net/netutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type Service struct {
	Port, WriteTimeout, ReadTimeout, ConnectionsLimit int
	router                                            *mux.Router
	server                                            *http.Server
	listener                                          net.Listener
}

type contextKeyType string

const (
	router       = contextKeyType("router")
	RouterPrefix = "router"
)

func getService(ctx context.Context) *Service {
	return ctx.Value(router).(*Service)
}

func (s *Service) loadOrPutConfig(ctx context.Context) error {
	err := iconfig.GetConfig(ctx, RouterPrefix, &s)
	if err != nil {
		gochips.Info("can't find router config in consul, use default")
		err = iconfig.PutConfig(ctx, RouterPrefix, &s)
		if err != nil {
			return err
		}
	}
	return nil
}

// Start s.e.
func (s *Service) Start(ctx context.Context) (context.Context, error) {
	err := s.loadOrPutConfig(ctx)
	if err != nil {
		return ctx, err
	}

	s.router = mux.NewRouter()

	port := strconv.Itoa(s.Port)
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return ctx, nil
	}

	if s.ConnectionsLimit > 0 {
		s.listener = netutil.LimitListener(s.listener, s.ConnectionsLimit)
	}

	s.server = &http.Server{
		Addr:         ":" + port,
		Handler:      s.router,
		ReadTimeout:  time.Duration(s.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(s.WriteTimeout) * time.Second,
	}

	s.RegisterHandlers(ctx)

	gochips.Info("Router started")
	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			gochips.Info(err)
		}
	}()

	c := make(chan os.Signal)

	signal.Notify(c, os.Interrupt)

	<-c

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.Stop(ctx)
	gochips.Info("Shutting down")
	os.Exit(0)
	return nil, nil
}

// Stop s.e.
func (s *Service) Stop(ctx context.Context) {
	err := s.server.Shutdown(ctx)
	if err != nil {
		s.listener.Close()
		s.server.Close()
	}
}
