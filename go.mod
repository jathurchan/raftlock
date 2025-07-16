module github.com/jathurchan/raftlock

go 1.23.0

toolchain go1.23.4

require (
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.5
)

require (
	golang.org/x/net v0.34.0
	golang.org/x/sys v0.29.0
	golang.org/x/text v0.21.0 
	golang.org/x/time v0.11.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f
)

replace github.com/jathurchan/raftlock => .
