#include <common/status.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>

namespace kv {
// 析构函数：释放之前 malloc 出来的内存，防止内存泄漏
Status::~Status() {
    if (status_) {
        free(status_);
    }
}

// 拷贝构造函数：深拷贝（把对方的内存复制一份给自己，避免两个对象共用一块内存）
Status::Status(const Status& s) { status_ = Copy(s); }

// 赋值运算符：先把旧内存释放掉，再拷贝新的
Status& Status::operator=(const Status& s) {
    if (status_ != nullptr) {
        free(status_);
    }
    status_ = Copy(s);
    return *this;
}

// 深拷贝辅助函数：按 [4字节长度][1字节code][消息] 的布局整体复制
char* Status::Copy(const Status& s) {
    if (s.status_ == nullptr) {
        // 对方是 Ok，没什么可拷的
        return nullptr;
    } else {
        //    4     1    6
        // [len-6][code][message............]
        uint32_t len;
        // 先读出前 4 字节 = 消息长度
        memcpy(&len, s.status_, sizeof(uint32_t));
        // 分配 len+5 字节：4(长度)+1(code)+len(消息)
        char* status = (char*)malloc(len + 5);
        // 整体拷贝
        memcpy(status, s.status_, len + 5);
        return status;
    }
}

// 把 Status 转成给人看的字符串，如 "Not Found: xxx"
std::string Status::ToString() const {
    if (IsOk()) {
        return "Ok";
    }

    const char* str;
    char tmp[30];
    Code c = code();
    switch (c) {
        case Code::STATUS_Ok:
            str = "Ok";
            break;
        case Code::STATUS_NotFound:
            str = "Not Found:";
            break;
        case Code::STATUS_NotSupported:
            str = "Not Supported:";
            break;
        case Code::STATUS_InvalidArgument:
            str = "Invalid Argument:";
            break;
        case Code::STATUS_IoError:
            str = "Io Error:";
            break;
        default: {
            // 理论上走不到的错误码，也打印出来
            snprintf(tmp, sizeof(tmp), "Unknown code(%d):", c);
            str = tmp;
        }
    }

    std::string ret(str);
    uint32_t length;
    // 再读一次消息长度
    memcpy(&length, status_, sizeof(length));

    if (length > 0) {
        // 跳过前 5 字节（长度 4 + code 1），把后面的消息 append 上
        ret.append(status_ + 5, length);
    } else {
        // 没有消息内容，把刚才加的 ":" 去掉
        ret.pop_back();
    }

    return ret;
}

// 私有构造函数：按 [4字节长度][1字节code][消息] 组装一块内存
Status::Status(Code code, const char* msg) {
    uint32_t len;
    if (msg == nullptr) {
        len = 0;
    } else {
        len = strlen(msg);
    }
    status_ = (char*)malloc(len + 5);
    // 先写消息长度
    memcpy(status_, &len, sizeof(uint32_t));
    // 再写错误码
    status_[4] = code;
    // 最后写消息内容
    memcpy(status_ + 5, msg, len);
}

}  // namespace kv
