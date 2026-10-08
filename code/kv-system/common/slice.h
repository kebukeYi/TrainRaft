#pragma once

#include <assert.h>
#include <stddef.h>
#include <string.h>

#include <string>

namespace kv {
// ============================================================================
// Slice：零拷贝的"字节切片"（就是 {指针, 长度} 两个字段）
//
// 大白话：在 Raft 代码里经常要比较两段字节是否相等（比如选举时要比较
// context 是不是"转移领导权"的标识）、要把一段字节传来传去。
// 如果每次都复制 std::string，性能差；Slice 只记"从哪开始、多长"，
// 不拥有数据、不复制数据，所以叫"零拷贝"。
// 注意：它只是一个"视图"，不负责释放内存，使用期间保证底层数据还活着即可。
// ============================================================================
class Slice {
   public:
    // 创建一个空切片（空字符串，长度 0）
    Slice() : data_(""), size_(0) {}

    // 创建一个引用 d[0,n-1] 的切片（指向外部数据，不复制）
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // 创建一个引用 std::string "s" 内容的切片
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // 创建一个引用 C 字符串的切片（自动算长度）
    Slice(const char* s) : data_(s), size_(strlen(s)) {}

    // 返回所引用数据起始位置的指针
    const char* Data() const { return data_; }

    // 返回引用数据的长度（字节数）
    size_t Size() const { return size_; }

    // 是否是 0 字节（空）
    bool Empty() const { return size_ == 0; }

    // 取出第 n 个字节（像数组一样用 []）
    char operator[](size_t n) const {
        assert(n < Size());
        return data_[n];
    }

    // 清空切片（改成空）
    void Clear() {
        data_ = "";
        size_ = 0;
    }

    // 丢掉前 n 个字节（只挪指针，不复制）
    void RemovePrefix(size_t n) {
        assert(n <= Size());
        data_ += n;
        size_ -= n;
    }

    // 复制出一份真正的 std::string（这里才发生拷贝）
    std::string ToString() const { return std::string(data_, size_); }

    // 三向比较（类似 strcmp）：返回 <0 / ==0 / >0
    int Compare(const Slice& b) const;

    // 判断 "x" 是不是 "*this" 的前缀（比如判断消息 context 是否以某串开头）
    bool StartsWith(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

   private:
    const char* data_;  // 指向数据的首地址（不拥有）
    size_t size_;       // 数据长度

    // 允许被复制（设计上有意如此：复制 Slice 只是复制"指针+长度"，很廉价）
};

// 全局 == 运算符：长度相同且内容相同才相等
inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.Size() == y.Size()) &&
            (memcmp(x.Data(), y.Data(), x.Size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

// 三向比较的实现：先比较公共长度的字节，再比较长度
inline int Slice::Compare(const Slice& b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_)
            r = -1;
        else if (size_ > b.size_)
            r = +1;
    }
    return r;
}

}  // namespace kv
