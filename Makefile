.PHONY: test
test:
	go test -mod=vendor -p 1 -timeout 20m -v -coverprofile=coverage.out -covermode=atomic -coverpkg=./... `go list -mod=vendor ./... | grep -v -E "(hasher|internal/bufpool|pkg/flog|pkg/neterrors|pkg/service_discovery)"`