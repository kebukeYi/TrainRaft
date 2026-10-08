#!/bin/bash
set -e

rootPath=/cxx_project/dkv
buildPath=${rootPath}/build

perfDataFile=$(basename "$1")
filePrefix=${perfDataFile%.*}

pprof-new --collapsed ${buildPath}/raft-app/raft-kv ${buildPath}/${filePrefix}.prof > ${buildPath}/${filePrefix}.folded

/opt/FlameGraph/flamegraph.pl ${buildPath}/${filePrefix}.folded > ${buildPath}/${filePrefix}.svg

echo "✅ 火焰图生成完成：${buildPath}/${filePrefix}.svg"