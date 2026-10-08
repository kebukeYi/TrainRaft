#pragma once
#ifndef UTILS_H
#define UTILS_H

#include <common/log.h>
#include <resource/raft.pb.h>

#include <boost/crc.hpp>
#include <cstdint>
#include <string>
#include <vector>

using namespace proto;
namespace kv {
    // ============================================================================
    // 通用小工具函数
    //
    // ComputeCrc32：计算一段字节的 CRC32 校验值。
    // 大白话：WAL 日志文件和快照文件写盘时，都会附带一个 CRC32 校验值；
    // 下次读出来时重新算一遍，如果对不上，说明文件写坏/读坏了（比如机器
    // 突然断电只写了一半），这时候就要丢弃或截断这段脏数据，保证一致性。
    // ============================================================================
uint32_t ComputeCrc32(const char* data, size_t len);
}

#endif /* UTILS_H */
