PROTO_DIR := proto
OUT_DIR := gen

.PHONY: proto clean build test

proto:
	mkdir -p $(OUT_DIR)/fileserverpb $(OUT_DIR)/metadatapb
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--proto_path=$(PROTO_DIR)/fileserver \
		--go_out=$(OUT_DIR)/fileserverpb --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR)/fileserverpb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/fileserver/fileserver.proto
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--proto_path=$(PROTO_DIR)/metadata \
		--go_out=$(OUT_DIR)/metadatapb --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR)/metadatapb --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/metadata/metadata.proto

build:
	go build ./cmd/metaserver ./cmd/fileserver

test:
	go test ./tests/... -v -count=1

clean:
	rm -rf $(OUT_DIR)
	go clean ./...
