package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	iconfig "github.com/untillpro/airs-iconfig"
	config "github.com/untillpro/airs-iconfigcon"
	iqueues "github.com/untillpro/airs-iqueues"
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
	queueAliasVar           = "queue-alias"
	partitionDividendVar    = "partition-dividend"
	resourceNameVar         = "resource-name"
	resourceIdVar           = "resource-id"
	noPartyVar              = "rest"
	consulPrefix            = "router"
	defaultNATServers       = "nats1,nats2,nats3"
	defaultPort             = "8080"
	defaultReadTimeout      = 10
	defaultWriteTimeout     = 10
	defaultConnectionsLimit = 10000
)

const (
	RouterPrefix = "router"
)

var queueNumberOfPartitions = make(map[string]int)

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
		queueRequest.Args = body
	}
	if queueRequest.PartitionDividend == 0 {
		http.Error(resp, "partition dividend in partitioned queues must be not 0", http.StatusBadRequest)
		return
	}
	queueRequest.PartitionNumber = queueRequest.PartitionDividend % numberOfPartitions
	iqueues.InvokeFromHTTPRequest(ctx, queueRequest, resp, iqueues.DefaultTimeout)
}

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

func addTestHandlers() {
	queueNumberOfPartitions["air-bo-view"] = 0
	queueNumberOfPartitions["air-bo"] = 10

	godif.ProvideKeyValue(&iqueues.NonPartyHandlers, "air-bo-view:0", queues.AirBoView)
	godif.ProvideKeyValue(&iqueues.PartitionHandlerFactories, "air-bo:10", queues.Factory)
}

//TODO pass configs throw flags or config file
func getConfiguredServices(ctx context.Context) (config.Service, queues.Service, Service) {

	var err error
	services.DeclareRequire()
	godif.Require(&iconfig.PutConfig)
	godif.Require(&iconfig.GetConfig)

	var confService = config.Service{Host: "127.0.0.1", Port: 8500}
	config.Declare(confService)
	ctx, err = confService.Start(ctx)
	if err != nil {
		gochips.Fatal(err)
	}

	errs := godif.ResolveAll()
	if errs != nil {
		gochips.Fatal(errs)
	}

	var queuesService queues.Service
	err = iconfig.GetConfig(ctx, queues.QueuesPrefix, &queuesService)
	if err != nil {
		gochips.Info("can't find queues config in consul, use default")
		queuesService = queues.Service{Servers: "0.0.0.0"}
		err = iconfig.PutConfig(ctx, queues.QueuesPrefix, &queuesService)
		if err != nil {
			gochips.Fatal(err)
		}
	}

	var routerService Service
	err = iconfig.GetConfig(ctx, RouterPrefix, &routerService)
	if err != nil {
		gochips.Info("can't find router config in consul, use default")
		routerService = Service{Port: 8822, WriteTimeout: 10, ReadTimeout: 10, ConnectionsLimit: -1}
		err = iconfig.PutConfig(ctx, RouterPrefix, &routerService)
		if err != nil {
			gochips.Fatal(err)
		}
	}

	godif.Reset()

	return confService, queuesService, routerService
}

func main() {

	ctx := context.Background()

	confService, queuesService, routerService := getConfiguredServices(ctx)

	//only for testing purposes
	addTestHandlers()

	services.DeclareRequire()
	godif.Require(&iqueues.InvokeFromHTTPRequest)

	//TODO in future we might need to provide put and get here to update configs automatically
	godif.ProvideSliceElement(&iservices.Services, &confService)
	queues.Declare(queuesService)
	Declare(routerService)

	err := iservices.Run()
	if err != nil {
		gochips.Info(err)
	}
}
