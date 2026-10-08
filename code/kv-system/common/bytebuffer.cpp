
#include <common/bytebuffer.h>
#include <string.h>

namespace kv {
// 初始缓冲区大小：4KB
static uint32_t MIN_BUFFERING = 4096;

// 构造函数：读写指针都从 0 开始，预先分配 4KB
ByteBuffer::ByteBuffer() : reader_(0), writer_(0), buff_(MIN_BUFFERING) {}

// 写数据：如果剩余空间不够，就把缓冲区扩大一倍再写
void ByteBuffer::Put(const uint8_t* data, uint32_t len) {
    uint32_t left = static_cast<uint32_t>(buff_.size()) - writer_;
    if (left < len) {
        // 空间不够：扩容（原大小*2+len，保证至少能放下 len 字节）
        buff_.resize(buff_.size() * 2 + len, 0);
    }
    // 从写指针位置开始拷贝
    memcpy(buff_.data() + writer_, data, len);
    writer_ += len;  // 写指针前进
}

// 可读字节数 = 写指针 - 读指针
uint32_t ByteBuffer::ReadableBytes() const {
    assert(writer_ >= reader_);
    return writer_ - reader_;
}

// 消费 bytes 字节（读走）：只移动读指针，并不真正删除数据
void ByteBuffer::ReadBytes(uint32_t bytes) {
    assert(ReadableBytes() >= bytes);
    reader_ += bytes;
    MayShrinkToFit();
}

// 优化：如果数据恰好被读空了，把两个指针都归零，
// 这样下次写入会从头开始，缓冲区不会越堆越乱
void ByteBuffer::MayShrinkToFit() {
    if (reader_ == writer_) {
        reader_ = 0;
        writer_ = 0;
    }
}

// 彻底重置：指针归零，容量缩回初始的 4KB
void ByteBuffer::Reset() {
    reader_ = writer_ = 0;
    buff_.resize(MIN_BUFFERING);
    buff_.shrink_to_fit();
}
}  // namespace kv
