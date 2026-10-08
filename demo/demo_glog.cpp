#include <glog/logging.h>  // 引入glog库

// 一个示例函数，演示如何使用新的日志宏
void exampleFunction(uint64_t id, uint64_t term) {
    LOG(INFO) << "%lu became pre-candidate at term %lu" << id << term;
    // LOG_FATAL("invalid transition [follower -> leader]");
}

int main(int argc, char** argv) {
    // 初始化glog库
    google::InitGoogleLogging(argv[0]);

    // 设置日志级别，例如设置为INFO级别
    FLAGS_minloglevel = 0;  // 设置最低日志级别为INFO
    FLAGS_logtostderr = 1;  // 将日志输出到标准错误流
    FLAGS_v = 1;            // 设置详细日志级别为1

    exampleFunction(123, 456);

    return 0;
}