#pragma once
#include <common/slice.h>
#include <stdint.h>
#include <string.h>

#include <vector>

namespace kv {

// ============================================================================
// ByteBuffer：带"读指针/写指针"的字节缓冲区
//
// 大白话：好比一个水桶，往里面倒水（Put=写），从里面舀水（ReadBytes=读）。
// 用两个指针管理：
//   - reader_：已经读到哪了
//   - writer_：已经写到哪了
//   - reader_ 和 writer_ 之间的数据 = 还没被读走的数据（ReadableBytes）
// 底层用 std::vector<uint8_t> 存字节。常用于把多个消息拼起来再统一读走。
// ============================================================================
class ByteBuffer {
   public:
    explicit ByteBuffer();

    // 写: 把 data 里 len 个字节追加到缓冲区末尾
    void Put(const uint8_t* data, uint32_t len);

    // 读: 把 reader_ 往前推 bytes 个字节（表示这 bytes 字节被消费掉了）
    void ReadBytes(uint32_t bytes);

    // 是否还有数据可读（写指针 > 读指针）
    bool Readable() const { return writer_ > reader_; }

    // 可读数据的字节数
    uint32_t ReadableBytes() const;

    // 缓冲区的总容量
    uint32_t Capacity() const {
        return static_cast<uint32_t>(buff_.capacity());
    }

    // 返回"当前可读数据"的首地址（就是读指针的位置）
    const uint8_t* Reader() const { return buff_.data() + reader_; }

    // 把可读部分直接包装成一个 Slice（零拷贝视图，方便丢给 protobuf 解析）
    Slice slice() const {
        return Slice((const char*)Reader(), ReadableBytes());
    }

    // 全部清空
    void Reset();

   private:
    // 内部优化：如果数据全被读完了，就把读写指针都归零，避免缓冲区越来越大
    void MayShrinkToFit();

    uint32_t reader_;            // 读指针：下一个要读的位置
    uint32_t writer_;            // 写指针：下一个要写的位置
    std::vector<uint8_t> buff_;  // 实际存字节的容器
};

}  // namespace kv
