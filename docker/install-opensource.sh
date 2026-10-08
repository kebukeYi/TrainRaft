#!/bin/bash

sed -e;

# 编译的CPU核心数
coreNum=4
if [ $# -ne 0 ]; then
  coreNum=$1
fi

BUILD_DIR=/opt

cd $BUILD_DIR
git clone --depth=1 https://github.com/gperftools/gperftools.git
git clone --depth=1 https://github.com/apache/brpc.git && cd brpc && git submodule update --init --recursive
git clone --depth=1 https://github.com/facebook/rocksdb.git

# sed -i 's/DEFINE_int32(v/DEFINE_int32(brpcv/g' /opt/brpc/src/butil/logging.cc
# sed -i 's/DEFINE_string(vmodule/DEFINE_string(brpcvmodule/g' /opt/brpc/src/butil/logging.cc
# sed -i 's/DEFINE_int32(minloglevel/DEFINE_int32(brpcminloglevel/g' /opt/brpc/src/butil/logging.cc
# sed -i 's/FLAGS_v/FLAGS_brpcv/g' /opt/brpc/src/butil/logging.cc
# sed -i 's/FLAGS_vmodule/FLAGS_brpcvmodule/g' /opt/brpc/src/butil/logging.cc
# sed -i 's/FLAGS_minloglevel/FLAGS_brpcminloglevel/g' /opt/brpc/src/butil/logging.cc

cp -r /opt/apache-zookeeper-3.8.6-bin/conf/zoo_sample.cfg  /opt/apache-zookeeper-3.8.6-bin/conf/zoo.cfg
sed -i 's/dataDir=\/tmp\/zookeeper/dataDir=\/opt\/zookeeperData/g' /opt/apache-zookeeper-3.8.6-bin/conf/zoo.cfg

cd $BUILD_DIR/gperftools && ./autogen.sh && ./configure --prefix=/usr && make && make install && ldconfig && make clean
cd $BUILD_DIR/brpc && cmake -S . -B build -DWITH_GLOG=OFF -DCMAKE_INSTALL_PREFIX=/usr/ -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j $coreNum && cmake --install build && rm -rf build
cd $BUILD_DIR/rocksdb && cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j $coreNum && cmake --install build && rm -rf build