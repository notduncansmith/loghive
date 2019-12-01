build:
	go build
testunit:
	go test -coverprofile=coverage.txt
testrace:
	go test -race
test: testunit testrace
all: build test
