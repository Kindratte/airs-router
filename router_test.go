package main

import (
	"github.com/gorilla/mux"
	"github.com/nats-io/go-nats"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testEntity struct {
	routeVar   string
	shouldPass bool
}

func createRouter() *GoonceRouter {
	r := mux.NewRouter()
	gr := GoonceRouter{&nats.Conn{}, r, &http.Server{}}
	return &gr
}

func assertForGet(tes []testEntity, t *testing.T, gr *GoonceRouter) {
	for _, tc := range tes {
		a := tc.routeVar
		req, err := http.NewRequest("GET", a, nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		gr.router.ServeHTTP(rr, req)
		if rr.Code == http.StatusOK && !tc.shouldPass {
			t.Errorf("handler should have failed on routeVariable %s",
				tc.routeVar)
		}
		if rr.Code != http.StatusOK && tc.shouldPass {
			t.Errorf("handler should have failed on routeVariable %s",
				tc.routeVar)
		}
	}
}

func TestGoonceRouter_HelpHandler(t *testing.T) {
	gr := createRouter()
	gr.router.HandleFunc("/{a}", gr.HelpHandler)
	gr.router.HandleFunc("/{a}/{b:[0-9]+}", gr.HelpHandler)
	tes := []testEntity{
		{"/abfdba/123123/aedasd", false},
		{"/adsf", true},
		{"/12414", true},
		{"/abfdba/asdfas", false},
		{"/abfdba/123123", true},
		{"/abfdba/asdas123123", false},
		{"/abfdba/123123/aedasd", false},
	}

	assertForGet(tes, t, gr)
}
