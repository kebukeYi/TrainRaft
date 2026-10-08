FROM ubuntu:22.04

# 默认工作目录锁定 /opt
WORKDIR /opt

# ========== 先替换apt阿里云源（顺序必须放在apt-get update之前） ==========
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list

#=== 基础库 + Java/Go/Python 全套依赖
RUN set -e; \
    apt-get update && apt-get install -y --no-install-recommends \
        curl apt-utils openssl build-essential ca-certificates pkg-config libglib2.0-dev valgrind \
        gdb vim automake autoconf libtool sudo git g++ gcc make libssl-dev dos2unix \
        libfmt-dev libssl-dev libevent-dev libdouble-conversion-dev libzstd-dev liblz4-dev \
        libsnappy-dev libjemalloc-dev ccache libjsoncpp-dev \
        libgflags-dev libgoogle-glog-dev libgtest-dev \
        libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libmsgpack-dev \
        lcov wget zlib1g zlib1g-dev libncurses5-dev liblzma-dev cmake \
        libboost-all-dev zookeeper libzookeeper-mt-dev \
        openjdk-11-jdk python3 python3-pip; \
    python3 -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple; \
    update-alternatives --install /usr/bin/python python /usr/bin/python3 1; \
    wget https://golang.google.cn/dl/go1.26.5.linux-amd64.tar.gz; \
    tar -C /usr/local -xzf go1.26.5.linux-amd64.tar.gz; \
    rm -rf go1.26.5.linux-amd64.tar.gz; \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 环境变量
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
ENV GOPROXY=https://goproxy.cn,direct
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV ANT_HOME=/usr
ENV PATH="$ANT_HOME/bin:$PATH"
ENV IVY_HOME=/usr/share/java
ENV PATH="$IVY_HOME:$PATH"


#=== 工具安装：Ivy、ChaosBlade、ZooKeeper、FlameGraph、Goreman
# 1. Ivy.jar 1335KB
RUN set -e; \
    wget http://maven.aliyun.com/nexus/content/groups/public/org/apache/ivy/ivy/2.5.2/ivy-2.5.2.jar \
    -O /usr/share/java/ivy.jar

# 2. ChaosBlade  54.7MB
RUN set -e; \
    wget https://github.com/chaosblade-io/chaosblade/releases/download/v1.7.4/chaosblade-1.7.4-linux-amd64.tar.gz && \
    tar -xzvf chaosblade-1.7.4-linux-amd64.tar.gz && \
    rm chaosblade-1.7.4-linux-amd64.tar.gz

# 3. ZooKeeper + 自动生成 zoo.cfg
RUN set -e; \
    wget https://dlcdn.apache.org/zookeeper/zookeeper-3.8.6/apache-zookeeper-3.8.6-bin.tar.gz && \
    tar -xzvf apache-zookeeper-3.8.6-bin.tar.gz && \
    rm apache-zookeeper-3.8.6-bin.tar.gz; \
    cp -r /opt/apache-zookeeper-3.8.6-bin/conf/zoo_sample.cfg  /opt/apache-zookeeper-3.8.6-bin/conf/zoo.cfg; \
    sed -i 's/dataDir=\/tmp\/zookeeper/dataDir=\/opt\/zookeeperData/g' /opt/apache-zookeeper-3.8.6-bin/conf/zoo.cfg

# 4. FlameGraph
RUN set -e; \
    git clone --depth=1 https://github.com/brendangregg/FlameGraph.git

# 5. Goreman
RUN set -e; \
    go install github.com/mattn/goreman@latest

# 6. pprof
RUN set -e; \
    go install github.com/google/pprof@latest && \
    ln -s /root/go/bin/pprof /usr/local/bin/pprof

#=== 源码克隆（分层缓存，单独失败直接终止）
RUN set -e; \
    git clone --depth=1 https://github.com/gperftools/gperftools.git

RUN set -e; \
    git clone --depth=1 https://github.com/apache/brpc.git && \
    cd brpc && git submodule update --init --recursive

RUN set -e; \
    git clone --depth=1 https://github.com/facebook/rocksdb.git

#=== 编译 gperftools
RUN set -e; \
    cd gperftools && \
    ./autogen.sh && \
    ./configure --prefix=/usr && \
    make -j4 && make install && \
    ldconfig && make clean

#=== 编译 brpc Debug
RUN set -e; \
    cd brpc && \
    cmake -S . -B build -DWITH_GLOG=ON -DCMAKE_INSTALL_PREFIX=/usr/ -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-Wno-deprecated-declarations" && \
    cmake --build build -j4 && \
    cmake --install build && \
    rm -rf build

#=== 编译 rocksdb Debug
RUN set -e; \
    cd rocksdb && \
    cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Debug && \
    cmake --build build -j4 && \
    cmake --install build && \
    rm -rf build

#=== 全局清理
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /opt
CMD ["/bin/bash"]