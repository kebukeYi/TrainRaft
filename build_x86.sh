#!/bin/bash

# 编译的CPU核心数
coreNum=4
if [ $# -ne 0 ]; then
  coreNum=$1
fi

workDir=/opt

rootPath=$(dirname "$(readlink -f "$0")")
echo $rootPath

resourcePath="$rootPath/code/kv-system/resource"
protoFiles=$(ls "$resourcePath")
# echo $protoFiles

# 清理旧的 pb.h 和 pb.cc 文件
echo "Cleaning old *.pb.h and *.pb.cc files..."
find "$resourcePath" -type f \( -name "*.pb.h" -o -name "*.pb.cc" \) -delete

# Generating C++ code
for protoFile in $protoFiles;
do
    # echo $resourcePath/$protoFile
    if [ -f "$resourcePath/$protoFile" ]; then
        echo "Generating C++ code for $protoFile..."
        protoc -I=$resourcePath --cpp_out=$resourcePath $resourcePath/$protoFile
    fi
done


# build
cmake -S $rootPath/code/ -B build -D CMAKE_C_COMPILER_LAUNCHER=ccache -D CMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build build --parallel $coreNum
