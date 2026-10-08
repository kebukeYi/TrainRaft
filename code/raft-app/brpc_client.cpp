#include <brpc/channel.h>

#include "resource/cli.pb.h"

int main(int argc, char* argv[]) {
    // 创建Channel，代表到服务器的连接;
    brpc::Channel channel;
    brpc::ChannelOptions options;
    // 设置超时等选项。
    options.timeout_ms = 1000;  // 设置超时为1秒;
    if (channel.Init("127.0.0.1:63791", &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
    /******************************************写操作******************************************/
    proto::SetRequest setRequest;
    proto::SetResponse setResponse;
    setRequest.set_key("name");
    setRequest.set_value("lzc");

    // 创建Controller来控制RPC行为;
    brpc::Controller setcntl;
    // 发起同步RPC调用; 对端server在: ClientServiceImpl::Set()中处理请求
    proto::ClientService_Stub setstub(&channel);
    // done: NULL
    setstub.Set(&setcntl, &setRequest, &setResponse, NULL);
    // 检查RPC调用是否成功;
    if (setcntl.Failed()) {
        LOG(ERROR) << "Set RPC failed: " << setcntl.ErrorText();
        return -1;
    }

    /******************************************读操作******************************************/
    proto::GetRequest getRequest;
    proto::GetResponse getResponse;
    getRequest.set_key("name");

    // 创建Controller来控制RPC行为;
    brpc::Controller getcntl;
    // 发起同步RPC调用;
    proto::ClientService_Stub getstub(&channel);
    getstub.Get(&getcntl, &getRequest, &getResponse, NULL);
    // 检查RPC调用是否成功;
    if (getcntl.Failed()) {
        LOG(ERROR) << "Get RPC failed: " << getcntl.ErrorText();
        return -1;
    }
    // 处理响应。
    LOG(INFO) << "readindex: " << getResponse.readindex()
              << " value: " << getResponse.value() << std::endl;

    /******************************************删除操作******************************************/
    proto::DelRequest delRequest;
    proto::DelResponse delResponse;
    delRequest.set_key("name");
    // 创建Controller来控制RPC行为
    brpc::Controller delcntl;
    // 发起同步RPC调用
    proto::ClientService_Stub delstub(&channel);
    delstub.Del(&delcntl, &delRequest, &delResponse, NULL);
    // 检查RPC调用是否成功
    if (delcntl.Failed()) {
        LOG(ERROR) << "Del RPC failed: " << delcntl.ErrorText();
        return -1;
    }
    // 处理响应
    LOG(INFO) << "delete message: " << delResponse.message() << std::endl;

    return 0;
}
