package main

import (
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/nats-io/go-nats"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/netutil"
)

const (
	queueAliasVar           = "queue-alias"
	partitionDividendVar    = "partition-dividend"
	resourceNameVar         = "resource-name"
	resourceIdVar           = "resource-id"
	consulPrefix            = "router"
	defaultNATServers       = "nats1,nats2,nats3"
	defaultPort             = "8080"
	defaultReadTimeout      = 10
	defaultWriteTimeout     = 10
	defaultConnectionsLimit = 10000
)

type GoonceRouter struct {
	nats   *nats.Conn
	router *mux.Router
	server *http.Server
	//TODO consul
}

func (gr *GoonceRouter) request(resp http.ResponseWriter, partitionNumber string, message []byte) {
	msg, err := gr.nats.Request(partitionNumber, message, 5*time.Second)
	if err != nil {
		if gr.nats.LastError() != nil {
			http.Error(resp, gr.nats.LastError().Error(), http.StatusRequestTimeout)
			return
		}
		http.Error(resp, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return
	}
	strData := string(msg.Data)
	_, err = fmt.Fprintf(resp, strData)
	if err != nil {
		http.Error(resp, http.StatusText(http.StatusRequestTimeout), http.StatusBadRequest)
	}
}

func calculatePartitionNumber(productAndLocation string) (int, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(productAndLocation))
	if err != nil {
		return 0, err
	}
	return int(h.Sum32() % 100), nil
}

func (gr *GoonceRouter) FullPathHandler(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	//queueName := vars[queueAliasVar]
	partitionDividend := vars[partitionDividendVar]
	resourceName := vars[resourceNameVar]
	//resourceId := vars[resourceIdVar]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(resp, "Can't read post body: "+string(body), http.StatusBadRequest)
		return
	}
	partitionNumber, err := calculatePartitionNumber(resourceName + partitionDividend)
	if err != nil {
		http.Error(resp, "Can't calculate partition number", http.StatusConflict)
		return
	}
	gr.request(resp, strconv.Itoa(partitionNumber), body)
}

func (gr *GoonceRouter) QueueNamesHandler(resp http.ResponseWriter, req *http.Request) {
	log.Println("in queue names")
	fmt.Fprintf(resp, "queue_names")
}

func (gr *GoonceRouter) HelpHandler(resp http.ResponseWriter, req *http.Request) {
	log.Println("in help")
	fmt.Fprintf(resp, "help")
}

func (gr *GoonceRouter) ResourceNameHandler(resp http.ResponseWriter, req *http.Request) {
	log.Println("in resource name")
	fmt.Fprintf(resp, "resourse_name")
}

func (gr *GoonceRouter) HelpWithResourceIdHandler(resp http.ResponseWriter, req *http.Request) {
	log.Println("in help with reousrce-id")
	fmt.Fprintf(resp, "help_resource_id")
}

func (gr *GoonceRouter) ResourceIdHandler(resp http.ResponseWriter, req *http.Request) {
	log.Println("in ok no part")
	fmt.Fprintf(resp, "ok")
}

func (gr *GoonceRouter) RegisterHandlers() {
	gr.router.HandleFunc("/", gr.QueueNamesHandler)
	gr.router.HandleFunc(fmt.Sprintf("/{%s}", queueAliasVar), gr.HelpHandler).
		Methods("GET")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar), gr.HelpHandler).
		Methods("GET")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/_/{%s:[0-9]+}", queueAliasVar, partitionDividendVar),
		gr.HelpWithResourceIdHandler).
		Methods("GET")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar,
		resourceIdVar), gr.HelpWithResourceIdHandler).
		Methods("GET")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}", queueAliasVar, partitionDividendVar,
		resourceNameVar), gr.ResourceNameHandler).
		Methods("GET", "POST", "PATCH")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/_/{%s:[a-zA-Z]+}", queueAliasVar, resourceNameVar), gr.ResourceNameHandler).
		Methods("GET", "POST", "PATCH").
		Queries(partitionDividendVar, fmt.Sprintf("{%s:[0-9]+}", partitionDividendVar))
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/_/{%s:[a-zA-Z]+}/{%s:[0-9]+}", queueAliasVar, resourceNameVar, resourceIdVar),
		gr.ResourceIdHandler).
		Methods("GET")
	gr.router.HandleFunc(fmt.Sprintf("/{%s}/{%s:[0-9]+}/{%s:[a-zA-Z]+}/{%s:[0-9]+}", queueAliasVar, partitionDividendVar,
		resourceNameVar, resourceIdVar), gr.ResourceIdHandler).
		Methods("GET")
}

func main() {
	//var nATSServers = flag.String("ns", defaultNATServers, "The nats server URLs (separated by comma)")
	var port = flag.String("p", defaultPort, "Server port")
	var readTimeout = flag.Int("rt", defaultReadTimeout, "Read timeout in seconds")
	var writeTimeout = flag.Int("wt", defaultWriteTimeout, "Write timeout in seconds")
	var connectionsLimit = flag.Int("cl", defaultConnectionsLimit, "Limit of incoming connections")

	flag.Parse()

	//opts := []nats.Option{nats.Name("Router")}
	//nc, err := nats.Connect(*nATSServers, opts...)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer nc.Close()

	nc := &nats.Conn{}

	r := mux.NewRouter()

	listener, err := net.Listen("tcp", ":"+defaultPort)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	listener = netutil.LimitListener(listener, *connectionsLimit)

	server := &http.Server{
		Addr:         ":" + *port,
		Handler:      r,
		ReadTimeout:  time.Duration(*readTimeout) * time.Second,
		WriteTimeout: time.Duration(*writeTimeout) * time.Second,
	}

	goonceRouter := GoonceRouter{nc, r, server}
	goonceRouter.RegisterHandlers()
	log.Println("Handlers registered")

	log.Println("Starting server...")
	if err := goonceRouter.server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
