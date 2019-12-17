build:
	go build
testunit:
	go test -v -coverprofile=coverage.txt
testrace:
	go test -v -race
test: testunit testrace
all: build test
