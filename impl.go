/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/untillpro/airs-iconfig"
	config "github.com/untillpro/airs-iconfigcon"
	"github.com/untillpro/airs-iqueues"
	queues "github.com/untillpro/airs-iqueuesnats"
	"github.com/untillpro/gochips"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/iservices"
	"github.com/untillpro/godif/services"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

const (
	//Gorilla mux params
	queueAliasVar        = "queue-alias"
	partitionDividendVar = "partition-dividend"
	resourceNameVar      = "resource-name"
	//Settings
	defaultRouterPort             = 8822
	defaultRouterReadTimeout      = 10
	defaultRouterWriteTimeout     = 10
	defaultRouterConnectionsLimit = 10000
)

var queueNumberOfPartitions = make(map[string]int)

//Handler partitioned requests
func (s *Service) PartitionedHandler(ctx context.Context, numberOfPartitions int, vars map[string]string, resp http.ResponseWriter, req *http.Request) {
	//hack for eugene
	if req.Method == http.MethodOptions {
		s.sendOptions(resp, req)
		return
	}
	//hack for eugene
	queueRequest, err := createRequest(req.Method, req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	queueRequest.Resource = vars[resourceNameVar]
	if req.Body != nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(resp, "can't read request body: "+string(body), http.StatusBadRequest)
			return
		}
		queueRequest.Body = strings.Replace(string(body), "'", "", -1)
	}
	if queueRequest.PartitionDividend == 0 {
		http.Error(resp, "partition dividend in partitioned queues must be not 0", http.StatusBadRequest)
		return
	}
	queueRequest.PartitionNumber = int(queueRequest.PartitionDividend % int64(numberOfPartitions))
	iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
}

//Handle no party requests
func (s *Service) NoPartyHandler(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
	//hack for eugene
	if req.Method == http.MethodOptions {
		s.sendOptions(resp, req)
		return
	}
	//hack for eugene
	vars := mux.Vars(req)
	alias := vars[queueAliasVar]
	numberOfPartitions := queueNumberOfPartitions[alias]
	if numberOfPartitions == 0 {
		pathSuffix := vars[resourceNameVar]
		queueRequest := &iqueues.Request{
			Method:            iqueues.NameToHTTPMethod[req.Method],
			QueueID:           alias + ":0",
			PartitionDividend: 0,
			PartitionNumber:   0,
			Resource:          pathSuffix,
			Query:             req.URL.Query(),
			Attachments:       map[string]string{},
		}
		if req.Body != nil {
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				http.Error(resp, "can't read request body: "+string(body), http.StatusBadRequest)
				return
			}
			queueRequest.Body = string(body)
		}
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	} else {
		http.Error(resp, "wrong route to no party handler", http.StatusBadRequest)
		return
	}
}

//Returns registered queue names
func (s *Service) QueueNamesHandler(resp http.ResponseWriter, req *http.Request) {
	//hack for eugene
	if req.Method == http.MethodOptions {
		s.sendOptions(resp, req)
		return
	}
	resp.Header().Set("Access-Control-Allow-Origin", "*")
	resp.Header().Set("Access-Control-Allow-Credentials", "true")
	resp.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS, PATCH")
	resp.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	//hack for eugene
	keys := make([]string, len(queueNumberOfPartitions))
	i := 0
	for k := range queueNumberOfPartitions {
		keys[i] = k
		i++
	}
	marshaled, err := json.Marshal(keys)
	if err != nil {
		http.Error(resp, "can't marshal queue aliases", http.StatusBadRequest)
	}
	_, err = fmt.Fprintf(resp, string(marshaled))
	if err != nil {
		http.Error(resp, "can't write response", http.StatusBadRequest)
	}
}

//Returns list of resources
func (s *Service) HelpHandler(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		//hack for eugene
		if req.Method == http.MethodOptions {
			s.sendOptions(resp, req)
			return
		}
		//hack for eugene
		queueRequest, err := createRequest(req.Method, req)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		queueRequest.Resource = fmt.Sprintf("%d", queueRequest.PartitionDividend)
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	}
}

func createRequest(reqMethod string, req *http.Request) (*iqueues.Request, error) {
	vars := mux.Vars(req)
	numberOfPartitions := queueNumberOfPartitions[vars[queueAliasVar]]
	partitionDividend := vars[partitionDividendVar]
	partitionDividendNum, err := strconv.ParseInt(partitionDividend, 10, 64)
	if err != nil {
		return nil, errors.New("wrong partition dividend " + partitionDividend)
	}
	return &iqueues.Request{
		Method:            iqueues.NameToHTTPMethod[reqMethod],
		QueueID:           vars[queueAliasVar] + ":" + strconv.Itoa(numberOfPartitions),
		PartitionDividend: partitionDividendNum,
		Query:             req.URL.Query(),
		Attachments:       map[string]string{},
	}, nil
}

func (s *Service) chooseHandler(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	numberOfPartitions := queueNumberOfPartitions[vars[queueAliasVar]]
	if numberOfPartitions == 0 {
		s.NoPartyHandler(ctx, resp, req)
	} else {
		s.PartitionedHandler(ctx, numberOfPartitions, vars, resp, req)
	}
}

func (s *Service) RegisterHandlers(ctx context.Context) {
	//Auth
	s.router.HandleFunc("/user/new", s.CreateAccount(ctx)).Methods("POST")
	s.router.HandleFunc("/user/login", s.Authenticate(ctx)).Methods("POST")
	//Auth
	s.router.HandleFunc("/", s.QueueNamesHandler)
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), s.HelpHandler(ctx)).
		Methods("GET", "OPTIONS")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}", queueAliasVar,
		partitionDividendVar, resourceNameVar), func(resp http.ResponseWriter, req *http.Request) {
		s.chooseHandler(ctx, resp, req)
	}).
		Methods("GET", "POST", "PATCH", "PUT", "OPTIONS")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[a-zA-Z]+}", queueAliasVar, resourceNameVar),
		func(resp http.ResponseWriter, req *http.Request) {
			s.NoPartyHandler(ctx, resp, req)
		}).
		Methods("GET", "POST", "PATCH", "PUT", "OPTIONS")
}

func addHandlers() {
	queueNumberOfPartitions["air-bo-view"] = 0
	queueNumberOfPartitions["air-bo"] = 10
	queueNumberOfPartitions["manifest"] = 0
}

func main() {
	var consulHost = flag.String("ch", config.DefaultConsulHost, "Consul server URL")
	var consulPort = flag.Int("cp", config.DefaultConsulPort, "Consul port")
	var natsServers = flag.String("ns", queues.DefaultNatsHost, "The nats server URLs (separated by comma)")
	var routerPort = flag.Int("p", defaultRouterPort, "Server port")
	var routerWriteTimeout = flag.Int("wt", defaultRouterWriteTimeout, "Write timeout in seconds")
	var routerReadTimeout = flag.Int("rt", defaultRouterReadTimeout, "Read timeout in seconds")
	var routerConnectionsLimit = flag.Int("cl", defaultRouterConnectionsLimit, "Limit of incoming connections")

	flag.Parse()
	gochips.Info("nats: " + *natsServers)
	gochips.Info("consul: " + *consulHost)

	services.DeclareRequire()

	godif.Require(&iconfig.PutConfig)
	godif.Require(&iconfig.GetConfig)
	godif.Require(&iqueues.InvokeFromHTTPRequest)
	godif.Require(&iqueues.Invoke)

	config.Declare(config.Service{Host: *consulHost, Port: uint16(*consulPort)})
	queues.Declare(queues.Service{Servers: *natsServers})
	Declare(Service{Port: *routerPort, WriteTimeout: *routerWriteTimeout, ReadTimeout: *routerReadTimeout,
		ConnectionsLimit: *routerConnectionsLimit})

	addHandlers()

	err := iservices.Run()
	if err != nil {
		gochips.Info(err)
	}
}

//hack for eugene
func (s *Service) sendOptions(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Access-Control-Allow-Origin", "*")
	resp.Header().Set("Access-Control-Allow-Credentials", "true")
	resp.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS, PATCH")
	resp.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	_, err := resp.Write([]byte{})
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
	}
}
