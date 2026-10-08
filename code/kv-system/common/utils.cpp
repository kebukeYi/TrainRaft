#include <common/utils.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace kv {
// 计算 CRC32 校验值：用 boost 现成的 crc_32 引擎，
// 把 data 里 len 个字节"喂"进去，最后取校验结果
uint32_t ComputeCrc32(const char* data, size_t len) {
    boost::crc_32_type crc32;
    crc32.process_bytes(data, len);
    return crc32();
}
}  // namespace kv
