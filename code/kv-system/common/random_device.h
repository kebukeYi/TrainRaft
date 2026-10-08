#pragma once
#include <random>

namespace kv {

// ============================================================================
// RandomDevice：随机数工具（只在 [min, max] 区间内取随机整数）
//
// 大白话：Raft 选举需要一个"随机化的选举超时时间"，防止所有节点同时超时、
// 同时发起选举导致永远选不出 leader（票数互相瓜分）。
// 这里就是那个随机数发生器：构造时给定区间，之后每次 Gen() 返回一个随机数。
// ============================================================================
class RandomDevice {
       public:
        // min/max 指定随机数范围（在 Raft 里就是 [electionTick, 2*electionTick-1]）
        explicit RandomDevice(uint32_t min, std::uint32_t max)
            : gen_(rd_()), distribution_(min, max) {}

        // 生成一个随机数
        uint32_t Gen();

       private:
        std::random_device rd_;                    // 真随机种子源
        std::mt19937 gen_;                         // 梅森旋转伪随机数引擎
        std::uniform_int_distribution<> distribution_;  // 均匀分布：保证范围内每个数等概率
};

}  // namespace kv
