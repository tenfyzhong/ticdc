#!/usr/bin/env bash

# ANSI color codes for styling the output
RED='\033[0;31m'    # Sets text to red
GREEN='\033[0;32m'  # Sets text to green
YELLOW='\033[0;33m' # Sets text to yellow
BLUE='\033[0;34m'   # Sets text to blue
NC='\033[0m'        # Resets the text color to default, no color

mkdir -p bin

if [ ! -x bin/minio ]; then
	# Download minio
	echo -e "${YELLOW} downloading minio...${NC}"
	wget -O bin/minio https://dl.min.io/server/minio/release/darwin-amd64/minio
	chmod +x bin/minio
fi

if [ ! -x bin/jq ]; then
	# Download jq using curl
	echo -e "${YELLOW} downloading jq...${NC}"
	wget -O bin/jq https://github.com/jqlang/jq/releases/download/jq-1.8.0/jq-macos-amd64
	chmod +x bin/jq
fi

if [ ! -d bin/bin ]; then
	echo -e "${YELLOW} downloading confluent...${NC}"
	wget -O bin/confluent-7.5.2.tar.gz https://packages.confluent.io/archive/7.5/confluent-7.5.2.tar.gz
	tar -C bin/ -xzf bin/confluent-7.5.2.tar.gz
	mv bin/confluent-7.5.2/bin/ bin/
	rm -rf confluent-7.5.2
fi

if [ ! -x bin/go-ycsb ]; then
	echo -e "${YELLOW} downloading go-ycsb...${NC}"
	wget -O bin/go-ycsb-darwin-amd64.tar.gz https://github.com/pingcap/go-ycsb/releases/latest/download/go-ycsb-darwin-amd64.tar.gz
	tar -C bin/ -xzf bin/go-ycsb-darwin-amd64.tar.gz
fi

if [ ! -x bin/etcdctl ]; then
	echo -e "${YELLOW} downloading etcd...${NC}"
	wget -O bin/etcd-v3.6.1-darwin-amd64.zip https://github.com/etcd-io/etcd/releases/download/v3.6.1/etcd-v3.6.1-darwin-amd64.zip
	unzip -d bin/ bin/etcd-v3.6.1-darwin-amd64.zip
	mv bin/etcd-v3.6.1-darwin-amd64/etcdctl bin/
fi

if [ ! -x bin/sync_diff_inspector ]; then
	echo -e "${YELLOW} downloading sync-diff-inspector...${NC}"
	wget -O bin/sync-diff-inspector-v9.0.0-beta.1-darwin-arm64.tar.gz https://tiup-mirrors.pingcap.com/sync-diff-inspector-v9.0.0-beta.1-darwin-arm64.tar.gz
	tar -C bin/ -xzf bin/sync-diff-inspector-v9.0.0-beta.1-darwin-arm64.tar.gz
fi

echo -e "${RED}You should copy the tidb binaries to the bin/ directory on your own.${NC}"
