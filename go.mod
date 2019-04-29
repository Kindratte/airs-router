module github.com/untillpro/goonce-router

go 1.12

require (
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/gorilla/mux v1.7.0
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/untillpro/airs-iconfig v0.0.0-20190426084518-0fb85c787737
	github.com/untillpro/airs-iconfigcon v0.0.0-20190426134748-29f812ffea04
	github.com/untillpro/airs-iqueues v0.0.0-20190426133418-81feb9ca9b8f
	github.com/untillpro/airs-iqueuesnats v0.0.0-20190426134112-b8c06cf1cf25
	github.com/untillpro/gochips v1.9.0
	github.com/untillpro/godif v0.9.0
	golang.org/x/net v0.0.0-20190320064053-1272bf9dcd53
)

replace github.com/untillpro/airs-iqueuesnats => ../airs-iqueuesnats

replace github.com/untillpro/airs-iqueues => ../airs-iqueues

replace github.com/untillpro/airs-iconfig => ../airs-iconfig

replace github.com/untillpro/airs-iconfigcon => ../airs-iconfigcon
