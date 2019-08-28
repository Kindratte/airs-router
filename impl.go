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
	"github.com/untillpro/godif/services"
	"io/ioutil"
	"net/http"
	"strconv"
)

const (
	//Gorilla mux params
	queueAliasVar        = "queue-alias"
	partitionDividendVar = "partition-dividend"
	resourceNameVar      = "resource-name"
	//Settings
	defaultRouterPort             = 8822
	defaultRouterConnectionsLimit = 10000
	//Timeouts should be greater than NATS timeouts to proper use in browser(multiply responses)
	defaultRouterReadTimeout  = 15
	defaultRouterWriteTimeout = 15
)

var queueNumberOfPartitions = make(map[string]int)

//Handler partitioned requests
func (s *Service) PartitionedHandler(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		numberOfPartitions := queueNumberOfPartitions[vars[queueAliasVar]]
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
			queueRequest.Body = string(body)
		}
		if queueRequest.PartitionDividend == 0 {
			http.Error(resp, "partition dividend in partitioned queues must be not 0", http.StatusBadRequest)
			return
		}
		queueRequest.PartitionNumber = int(queueRequest.PartitionDividend % int64(numberOfPartitions))
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	}
}

//Handle no party requests
func (s *Service) NoPartyHandler(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
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
}

//Returns registered queue names
func (s *Service) QueueNamesHandler() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
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
}

//Returns list of resources
func (s *Service) HelpHandler(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
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

func (s *Service) RegisterHandlers(ctx context.Context) {
	//Auth
	s.router.HandleFunc("/api/user/new", corsHandler(s.CreateAccount(ctx))).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/api/user/login", corsHandler(s.Authenticate(ctx))).Methods("POST", "OPTIONS")
	//Auth
	s.router.HandleFunc("/api", corsHandler(s.QueueNamesHandler()))
	s.router.HandleFunc(fmt.Sprintf("/api/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), corsHandler(s.HelpHandler(ctx))).
		Methods("GET", "OPTIONS")
	s.router.HandleFunc(fmt.Sprintf("/api/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), corsHandler(s.PartitionedHandler(ctx))).
		Methods("POST", "OPTIONS")
	s.router.HandleFunc(fmt.Sprintf("/api/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z_]+}", queueAliasVar,
		partitionDividendVar, resourceNameVar), corsHandler(s.PartitionedHandler(ctx))).
		Methods("GET", "POST", "PATCH", "PUT", "OPTIONS")
	s.router.HandleFunc(fmt.Sprintf("/api/{%s}/{%s:[a-zA-Z_]+}", queueAliasVar, resourceNameVar), corsHandler(s.NoPartyHandler(ctx))).
		Methods("GET", "POST", "PATCH", "PUT", "OPTIONS")
}

func addHandlers() {
	queueNumberOfPartitions["air-bo-view"] = 0
	queueNumberOfPartitions["air-bo"] = 100
	queueNumberOfPartitions["modules"] = 0
	//TEST
	queueNumberOfPartitions["test"] = 100
	queueNumberOfPartitions["test-cas"] = 100
	//TEST
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

	godif.Require(&iconfig.PutConfig)
	godif.Require(&iconfig.GetConfig)

	godif.Require(&iqueues.InvokeFromHTTPRequest)
	godif.Require(&iqueues.Invoke)

	config.Declare(config.Service{Host: *consulHost, Port: uint16(*consulPort)})
	queues.Declare(queues.Service{Servers: *natsServers})
	Declare(Service{Port: *routerPort, WriteTimeout: *routerWriteTimeout, ReadTimeout: *routerReadTimeout,
		ConnectionsLimit: *routerConnectionsLimit})

	addHandlers()

	err := services.Run()
	if err != nil {
		gochips.Info(err)
	}
}

func corsHandler(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(w)
		if r.Method == "OPTIONS" {
			return
		} else {
			h.ServeHTTP(w, r)
		}
	}
}

func setupResponse(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, PATCH")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization")
}
