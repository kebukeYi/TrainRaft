#include <common/random_device.h>

namespace kv {

// 生成一个随机数：直接调用均匀分布，返回落在 [min, max] 内的整数
uint32_t RandomDevice::Gen() {
    return static_cast<uint32_t>(distribution_(gen_));
}

}  // namespace kv
