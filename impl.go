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
	in10n "github.com/untillpro/airs-in10n"
	"github.com/untillpro/airs-iqueues"
	queues "github.com/untillpro/airs-iqueuesnats"
	"github.com/untillpro/gochips"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/iservices"
	"github.com/untillpro/godif/services"
	"io/ioutil"
	"net/http"
	"strconv"
)

const (
	queueAliasVar                 = "queue-alias"
	partitionDividendVar          = "partition-dividend"
	resourceNameVar               = "resource-name"
	resourceIdVar                 = "resource-id"
	noPartyVar                    = "rest"
	defaultRouterPort             = 8822
	defaultRouterReadTimeout      = 10
	defaultRouterWriteTimeout     = 10
	defaultRouterConnectionsLimit = 10000
)

var queueNumberOfPartitions = make(map[string]int)

//Handler partitioned requests
func (s *Service) PartitionedHandler(ctx context.Context, numberOfPartitions int, vars map[string]string, resp http.ResponseWriter, req *http.Request) {
	queueRequest, err := createRequest(req.Method, vars)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	resourceName := vars[resourceNameVar]
	resourceId := vars[resourceIdVar]
	queueRequest.Resource = fmt.Sprintf("%d/%s/%s", queueRequest.PartitionDividend, resourceName, resourceId)
	if req.Body != nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(resp, "can't read request body: "+string(body), http.StatusBadRequest)
			return
		}
		queueRequest.Args = string(body)
	}
	if queueRequest.PartitionDividend == 0 {
		http.Error(resp, "partition dividend in partitioned queues must be not 0", http.StatusBadRequest)
		return
	}
	queueRequest.PartitionNumber = queueRequest.PartitionDividend % numberOfPartitions
	iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
}

//Handle no party requests
func (s *Service) NoPartyHandler(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	alias := vars[queueAliasVar]
	numberOfPartitions := queueNumberOfPartitions[alias]
	if numberOfPartitions == 0 {
		pathSuffix := vars[noPartyVar]
		queueRequest := &iqueues.Request{
			Method:            iqueues.NameToHTTPMethod[req.Method],
			QueueID:           alias + ":0",
			PartitionDividend: 0,
			PartitionNumber:   0,
			Resource:          pathSuffix,
		}
		if req.Body != nil {
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				http.Error(resp, "can't read request body: "+string(body), http.StatusBadRequest)
				return
			}
			queueRequest.Args = body
		}
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	} else {
		http.Error(resp, "wrong route to no party handler", http.StatusBadRequest)
		return
	}
}

//Returns registered queue names
func (s *Service) QueueNamesHandler(resp http.ResponseWriter, req *http.Request) {
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
		vars := mux.Vars(req)
		queueRequest, err := createRequest(req.Method, vars)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		queueRequest.Resource = fmt.Sprintf("%d", queueRequest.PartitionDividend)
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	}
}

//Describes given resource
func (s *Service) HelpWithResourceIdHandler(ctx context.Context) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		resourceId := vars[resourceIdVar]
		queueRequest, err := createRequest(req.Method, vars)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		queueRequest.Resource = fmt.Sprintf("%d/%s", queueRequest.PartitionDividend, resourceId)
		iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
	}
}

func createRequest(reqMethod string, vars map[string]string) (*iqueues.Request, error) {
	numberOfPartitions := queueNumberOfPartitions[vars[queueAliasVar]]
	partitionDividend := vars[partitionDividendVar]
	partitionDividendNum, err := strconv.Atoi(partitionDividend)
	if err != nil {
		return nil, errors.New("wrong partition dividend " + partitionDividend)
	}
	return &iqueues.Request{
		Method:            iqueues.NameToHTTPMethod[reqMethod],
		QueueID:           vars[queueAliasVar] + ":" + strconv.Itoa(numberOfPartitions),
		PartitionDividend: partitionDividendNum,
	}, nil
}

func (s *Service) RegisterHandlers(ctx context.Context) {
	s.router.HandleFunc("/", s.QueueNamesHandler)
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), s.HelpHandler(ctx)).
		Methods("GET")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar,
		resourceIdVar), s.HelpWithResourceIdHandler(ctx)).
		Methods("GET")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar,
		resourceNameVar, resourceIdVar), func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		numberOfPartitions := queueNumberOfPartitions[vars[queueAliasVar]]
		if numberOfPartitions == 0 {
			s.NoPartyHandler(ctx, resp, req)
		} else {
			s.PartitionedHandler(ctx, numberOfPartitions, vars, resp, req)
		}
	}).
		Methods("GET", "POST")
	s.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[a-zA-Z0-9=\\-\\/]+}", queueAliasVar, noPartyVar),
		func(resp http.ResponseWriter, req *http.Request) {
			s.NoPartyHandler(ctx, resp, req)
		}).
		Methods("GET", "POST")
}

//TEST
func addTestHandlers() {
	queueNumberOfPartitions["air-bo-view"] = 0
	queueNumberOfPartitions["air-bo"] = 10
	queueNumberOfPartitions["subscribe"] = 1
	queueNumberOfPartitions["notify"] = 1

	godif.ProvideKeyValue(&iqueues.NonPartyHandlers, "air-bo-view:0", iqueues.AirBoView)
	godif.ProvideKeyValue(&iqueues.PartitionHandlerFactories, "air-bo:10", iqueues.Factory)
}

//TEST

func main() {
	var consulHost = flag.String("ch", config.DefaultConsulHost, "Consul server URL")
	var consulPort = flag.Int("cp", config.DefaultConsulPort, "Consul port")
	var natsServers = flag.String("ns", queues.DefaultNatsHost, "The nats server URLs (separated by comma)")
	var routerPort = flag.Int("p", defaultRouterPort, "Server port")
	var routerWriteTimeout = flag.Int("wt", defaultRouterWriteTimeout, "Write timeout in seconds")
	var routerReadTimeout = flag.Int("rt", defaultRouterReadTimeout, "Read timeout in seconds")
	var routerConnectionsLimit = flag.Int("cl", defaultRouterConnectionsLimit, "Limit of incoming connections")
	flag.Parse()

	services.DeclareRequire()

	godif.Require(&iconfig.PutConfig)
	godif.Require(&iconfig.GetConfig)
	godif.Require(&iqueues.InvokeFromHTTPRequest)
	godif.Require(&iqueues.Invoke)

	config.Declare(config.Service{Host: *consulHost, Port: uint16(*consulPort)})
	in10n.Declare(in10n.Service{Port: 8877, WriteTimeout: *routerWriteTimeout, ReadTimeout: *routerReadTimeout,
		ConnectionsLimit: *routerConnectionsLimit})
	queues.Declare(queues.Service{Servers: *natsServers})
	Declare(Service{Port: *routerPort, WriteTimeout: *routerWriteTimeout, ReadTimeout: *routerReadTimeout,
		ConnectionsLimit: *routerConnectionsLimit})

	addTestHandlers()

	err := iservices.Run()
	if err != nil {
		gochips.Info(err)
	}
}
