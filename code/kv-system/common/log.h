#pragma once

#include <string.h>
#include <stdio.h>
#include <string>
#include <exception>
#include <stdexcept>
#include <glog/logging.h>

// ============================================================================
// 日志宏：全系统统一打印日志的入口（底层用的是 google 的 glog 库）
//
// 大白话：代码里到处用 LOG_INFO / LOG_WARN / LOG_ERROR / LOG_DEBUG，
// 用法跟 printf 一样： LOG_INFO("raft %lu became leader", id);
// 区别是它会自动带上文件名、行号、时间，并且按级别过滤输出。
// 注意底层 glog 的 LOG(INFO) 不支持 printf 的 %d 占位符，
// 所以这里先 snprintf 格式化到栈上的 buffer，再喂给 glog。
// ============================================================================

// 调试日志：级别最低，只在需要排查问题时开
#define LOG_DEBUG(format, ...)                                          \
     {                                                                  \
          char buffer[1024];                                            \
          snprintf(buffer, sizeof(buffer), format "\n", ##__VA_ARGS__); \
          LOG(INFO) << buffer;                                          \
     }

// 普通信息日志：正常的流程日志（选主、收发消息等）
#define LOG_INFO(format, ...)                                           \
     do                                                                 \
     {                                                                  \
          char buffer[1024];                                            \
          snprintf(buffer, sizeof(buffer), format "\n", ##__VA_ARGS__); \
          LOG(INFO) << buffer;                                          \
     } while (0)

// 警告日志：不致命但需要注意的情况（如消息被丢弃）
#define LOG_WARN(format, ...)                                           \
     do                                                                 \
     {                                                                  \
          char buffer[1024];                                            \
          snprintf(buffer, sizeof(buffer), format "\n", ##__VA_ARGS__); \
          LOG(WARNING) << buffer;                                       \
     } while (0)

// 错误日志：出错了但程序还能继续跑（如某个 RPC 失败）
#define LOG_ERROR(format, ...)                                          \
     do                                                                 \
     {                                                                  \
          char buffer[1024];                                            \
          snprintf(buffer, sizeof(buffer), format "\n", ##__VA_ARGS__); \
          LOG(ERROR) << buffer;                                         \
     } while (0)

// 致命日志：打印完直接终止程序（glog 的 FATAL 会 abort）。
// 代码里大量用于"理论上不该发生的情况"，一旦发生说明有 bug 或数据损坏
#define LOG_FATAL(format, ...)                                          \
     do                                                                 \
     {                                                                  \
          char buffer[1024];                                            \
          snprintf(buffer, sizeof(buffer), format "\n", ##__VA_ARGS__); \
          LOG(FATAL) << buffer;                                         \
     } while (0)
