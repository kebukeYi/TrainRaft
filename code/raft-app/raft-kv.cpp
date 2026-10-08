#include <apply/raft-node/raft_node.h>
#include <common/log.h>
#include <gflags/gflags.h>
#include <jsoncpp/json/json.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

std::string ReadFile(const fs::path& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filePath.string());
    }
    std::string content((std::istreambuf_iterator<char>(file)),std::istreambuf_iterator<char>());
    return content;
}

// 定义命令行参数
DEFINE_uint64(id, 0, "Node ID");
DEFINE_string(
    configFile, "kv-system-config.json",
    "kv-system config file peers in the format of [kv-system-config.json]");

void PrintUsage(const char* program_name) {
    LOG(ERROR) << "Usage: " << program_name
               << " --configFile=kv-system-config.json";
    LOG(ERROR) << "  --id <node-id>        Node ID";
    LOG(ERROR) << "  --cluster <cluster-peers>  kv-system config file peers in "
                  "the format of [kv-system-config.json]";
}

int main(int argc, char* argv[]) {
    // 初始化 gflags 库
    google::ParseCommandLineFlags(&argc, &argv, true);
    // --id 1 --configFile=kv-system-config.json
    google::InitGoogleLogging(argv[0]);

    // 设置日志级别，例如设置为INFO级别
    FLAGS_minloglevel = 0;  // 设置最低日志级别为INFO
    FLAGS_logtostderr = 1;  // 将日志输出到标准错误流
    FLAGS_v = 1;            // 设置详细日志级别为1

    // 检查参数是否有效
    if (FLAGS_id == 0 || FLAGS_configFile.empty()) {
        PrintUsage(argv[0]);
        exit(EXIT_FAILURE);
    }

    fs::path filePath = FLAGS_configFile.c_str();

    Json::Value root;
    Json::Reader reader;
    bool parseRes = reader.parse(ReadFile(filePath), root);

    if (!parseRes) {
        LOG_ERROR("Error parsing JSON");
        return 1;
    }

    if (!root.isObject()) {
        return 1;
    }

    // 访问 cluster 数组
    std::vector<std::string> cluster;
    for (const auto& host : root["cluster"]) {
        cluster.push_back(host.asString());
    }
    std::cout << std::endl;

    std::string originDataDir;
    Json::Value& originDirValue = root["originDataDir"];
    if (originDirValue.isNull() || originDirValue.asString().empty()) {
        originDataDir = "/data";
    } else {
        originDataDir = originDirValue.asString();
    }

    kv::RaftNode::Main(FLAGS_id, originDataDir, cluster);

    return 0;
}
