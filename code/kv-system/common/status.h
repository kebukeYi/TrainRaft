#pragma once
#include <string>

namespace kv {

// ============================================================================
// Status：全系统的"错误码+错误消息"封装（仿 LevelDB 的做法）
//
// 大白话：Raft 和 KV 代码里到处都要返回"成功/失败"，但又想带上一点出错原因。
// 如果每个函数都返回 int
// 错误码，别人看不懂；直接抛异常又太慢（这里要追求性能）。 所以用 Status
// 这种"轻量对象"：一个指针搞定——指针为空 = 成功(Ok)， 指针非空 =
// 失败，指针里存着 [消息长度 | 错误码 | 具体消息]。 用法示例： if
// (!status.IsOk()) { LOG_ERROR("%s", status.ToString().c_str()); }
// ============================================================================
class Status {
   public:
    // 默认构造：status_ 为空指针 = Ok（成功）
    // 注释里的"字符数组首地址为空"就是指 status_ == nullptr
    Status() : status_(nullptr) {}

    Status(const Status& s);

    Status& operator=(const Status& s);

    ~Status();

    // 静态工厂方法：直接返回一个空对象，省去临时变量的开销
    static Status Ok() { return Status(); }

    static Status NotFound(const char* msg) {
        return Status(Code::STATUS_NotFound, msg);
    }

    static Status NotSupported(const char* msg) {
        return Status(STATUS_NotSupported, msg);
    }

    static Status InvalidArgument(const char* msg) {
        return Status(STATUS_InvalidArgument, msg);
    }

    static Status IoError(const char* msg) {
        return Status(STATUS_IoError, msg);
    }

    // 判断结果：status_ == nullptr 就是成功
    bool IsOk() const { return status_ == nullptr; }

    // 各种错误类型的判断，例如调 storage_->FirstIndex 时
    // 日志被压缩（compact）了，就会返回 NotFound 或 InvalidArgument
    bool IsNotFound() const { return code() == Code::STATUS_NotFound; }

    bool IsIoError() const { return code() == Code::STATUS_IoError; }

    bool IsNotSupported() const { return code() == STATUS_NotSupported; }

    bool IsInvalidArgument() const { return code() == STATUS_InvalidArgument; }

    // 转成字符串，方便打印日志，例如 "Not Found: xxx"
    std::string ToString() const;

   private:
    // 内部的错误码枚举（藏在 private，外面只用 IsXxx 判断）
    enum Code {
        STATUS_Ok = 0,
        STATUS_NotFound = 1,
        STATUS_NotSupported = 2,
        STATUS_InvalidArgument = 3,
        STATUS_IoError = 4
    };

    inline static char* Copy(const Status& s);

    // 私有构造函数：只有上面的静态工厂方法才能调用
    Status(Code code, const char* msg);

    // 取出错误码：从 status_ 内存布局里第 5 个字节读出来
    Code code() const {
        return status_ == nullptr ? Code::STATUS_Ok
                                  : static_cast<Code>(status_[4]);
    }

   private:
    // 内存布局（最重要的部分，看懂这个就懂 Status 了）：
    // state_[0..3] == message 的长度（4 字节）
    // state_[4]    == code（错误码，1 字节）
    // state_[5..]  == message（具体错误消息的字符）
    // 用 malloc 分配的一块连续内存，自己管理，所以析构要 free
    char* status_;
};

}  // namespace kv
