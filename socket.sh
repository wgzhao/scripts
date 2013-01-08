#!/bin/bash
host=${1:-"blog.wgzhao.com"}
echo "connect host:$host"
ssh -D7070 -nTfN upload@${host}
