/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/untillpro/airs-iqueues"
	"io/ioutil"
	"net/http"
	"strconv"
)

const (
	queueAliasVar        = "queue-alias"
	partitionDividendVar = "partition-dividend"
	resourceNameVar      = "resource-name"
	resourceVar          = "resource"
)

var QueueNumberOfPartitions = make(map[string]int)

//Handler partitioned requests
func (s *Service) PartitionedHandler(ctx context.Context, numberOfPartitions int, vars map[string]string, resp http.ResponseWriter, req *http.Request) {
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

//Handle no party requests
func (s *Service) NoPartyHandler(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	alias := vars[queueAliasVar]
	numberOfPartitions := QueueNumberOfPartitions[alias]
	if numberOfPartitions == 0 {
		pathSuffix := vars[resourceVar]
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
	keys := make([]string, len(QueueNumberOfPartitions))
	i := 0
	for k := range QueueNumberOfPartitions {
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
	numberOfPartitions := QueueNumberOfPartitions[vars[queueAliasVar]]
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
	numberOfPartitions := QueueNumberOfPartitions[vars[queueAliasVar]]
	if numberOfPartitions == 0 {
		s.NoPartyHandler(ctx, resp, req)
	} else {
		s.PartitionedHandler(ctx, numberOfPartitions, vars, resp, req)
	}
}

func (s *Service) RegisterHandlers(ctx context.Context) {
	s.router.HandleFunc("/", s.QueueNamesHandler)
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), s.HelpHandler(ctx)).
		Methods("GET")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}?{%s:[a-zA-Z0-9=\\-\\/]+}", queueAliasVar,
		partitionDividendVar, resourceNameVar, resourceVar), func(resp http.ResponseWriter, req *http.Request) {
		s.chooseHandler(ctx, resp, req)
	}).
		Methods("GET", "POST", "PATCH", "PUT")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}", queueAliasVar,
		partitionDividendVar, resourceNameVar), func(resp http.ResponseWriter, req *http.Request) {
		s.chooseHandler(ctx, resp, req)
	}).
		Methods("GET", "POST", "PATCH", "PUT")
	s.router.HandleFunc(fmt.Sprintf("/{%s}", queueAliasVar),
		func(resp http.ResponseWriter, req *http.Request) {
			s.NoPartyHandler(ctx, resp, req)
		}).
		Methods("GET", "POST", "PATCH", "PUT")
	s.router.HandleFunc(fmt.Sprintf("/{%s}?{%s:[a-zA-Z0-9=\\-\\/]+}", queueAliasVar, resourceVar),
		func(resp http.ResponseWriter, req *http.Request) {
			s.NoPartyHandler(ctx, resp, req)
		}).
		Methods("GET", "POST", "PATCH", "PUT")
}
