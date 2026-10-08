#include <gtest/gtest.h>
#include <raft/node.h>
#include <raft/util.h>
#include "network.hpp"

using namespace kv;

static bool read_state_cmp(const std::vector<ReadState> &l, const std::vector<ReadState> &r)
{
  if (l.size() != r.size())
  {
    return false;
  }

  for (size_t i = 0; i < l.size(); ++i)
  {
    if (l[i].index != r[i].index)
    {
      return false;
    }

    if (l[i].requestCtx != r[i].requestCtx)
    {
      return false;
    }
  }
  return true;
}

TEST(test_rawnode, NodeImplStep)
{
  for (uint8_t i = 0; i < proto::MsgTypeSize; ++i)
  {
    MemoryStoragePtr s(new MemoryStorage());
    auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);

    std::vector<PeerContext> nodes{PeerContext{.id = 1}};
    NodeImpl node(c, nodes);
    proto::MessagePtr msg(new proto::Message());
    msg->type = i;
    Status status = node.Step(msg);

    // LocalMsg should be ignored.
    if (IsLocalMsg(i))
    {
      ASSERT_TRUE(!status.IsOk());
    }
  }
}

// NodeImplProposeAndConfChange ensures that NodeImpl.Propose and NodeImpl.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
/*TEST(test_NodeImpl, NodeImplProposeAndConfChange)
{
  MemoryStoragePtr s(new MemoryStorage());
  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);
  std::vector<PeerContext> peer{PeerContext{.id = 1}};
  NodeImpl NodeImpl(c, peer);
  auto rd = NodeImpl.GetReady();
  s->Append(rd->entries);
  NodeImpl.Advance(rd);

  auto d = NodeImpl.GetReady();
  ASSERT_TRUE(d->hardState.IsEmptyState());
  ASSERT_TRUE(d->entries.empty());

  NodeImpl.Campaign();
  bool proposed = false;

  uint64_t lastIndex = 0;
  std::vector<uint8_t> ccdata;
  while (true)
  {
    rd = NodeImpl.GetReady();
    s->Append(rd->entries);
    // Once we are the leader, propose a command and a ConfChange.
    if (!proposed && rd->softState->lead == NodeImpl.raft_->id_)
    {
      NodeImpl.Propose(str_to_vector("somedata"));

      proto::ConfChange cc;
      cc.confChangeType = proto::ConfChangeAddNode;
      cc.nodeId = 1;
      ccdata = cc.Serialize();

      NodeImpl.ProposeConfChange(cc);

      proposed = true;
    }
    NodeImpl.Advance(rd);

    // Exit when we have four entries: one ConfChange, one no-op for the election,
    // our proposed command and proposed ConfChange.
    Status status = s->LastIndex(lastIndex);
    ASSERT_TRUE(status.IsOk());
    if (lastIndex >= 4)
    {
      break;
    }
  }

  std::vector<proto::EntryPtr> entries;
  Status status = s->Entries(lastIndex - 1, lastIndex + 1, RaftLog::Unlimited(), entries);
  ASSERT_TRUE(status.IsOk());
  ASSERT_TRUE(entries.size() == 2);

  ASSERT_TRUE(entries[0]->data == str_to_vector("somedata"));
  ASSERT_TRUE(entries[1]->type == proto::EntryConfChange);
  ASSERT_TRUE(entries[1]->data == ccdata);
}*/
/*
TEST(test_NodeImpl, NodeImplProposeAddDuplicateNode)
{
  MemoryStoragePtr s(new MemoryStorage());
  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);
  std::vector<PeerContext> peer{PeerContext{.id = 1}};
  NodeImpl NodeImpl(c, peer);

  auto rd = NodeImpl.GetReady();
  s->Append(rd->entries);
  NodeImpl.Advance(rd);

  NodeImpl.Campaign();
  while (true)
  {
    rd = NodeImpl.GetReady();
    s->Append(rd->entries);

    if (rd->softState->lead == NodeImpl.raft_->id_)
    {
      NodeImpl.Advance(rd);
      break;
    }
    NodeImpl.Advance(rd);
  }

  auto proposeConfChangeAndApply = [&NodeImpl, s](const proto::ConfChange &cc)
  {
    NodeImpl.ProposeConfChange(cc);
    auto rd = NodeImpl.GetReady();
    s->Append(rd->entries);

    for (auto entry : rd->committedEntries)
    {
      if (entry->type == proto::EntryConfChange)
      {
        proto::ConfChange cc;
        proto::ConfChange::FromData(entry->data, cc);
        // NodeImpl.ApplyConfChange(cc);
      }
    }
    NodeImpl.Advance(rd);
  };

  proto::ConfChange cc1;
  cc1.confChangeType = proto::ConfChangeAddNode;
  cc1.nodeId = 1;
  auto ccdata1 = cc1.Serialize();

  proposeConfChangeAndApply(cc1);

  // try to add the same node again
  proposeConfChangeAndApply(cc1);

  // the new node join should be ok
  proto::ConfChange cc2;
  cc2.confChangeType = proto::ConfChangeAddNode;
  cc2.nodeId = 2;
  auto ccdata2 = cc2.Serialize();

  proposeConfChangeAndApply(cc2);

  uint64_t lastIndex;
  Status status = s->LastIndex(lastIndex);
  ASSERT_TRUE(status.IsOk());

  // the last three entries should be: ConfChange cc1, cc1, cc2

  std::vector<proto::EntryPtr> entries;
  status = s->Entries(lastIndex - 2, lastIndex + 1, RaftLog::Unlimited(), entries);
  ASSERT_TRUE(status.IsOk());

  ASSERT_TRUE(entries.size() == 3);

  ASSERT_TRUE(entries[0]->data == ccdata1);

  ASSERT_TRUE(entries[2]->data == ccdata2);
}*/

TEST(test_NodeImpl, NodeImplReadIndex)
{
  std::vector<proto::MessagePtr> msgs;
  auto appendStep = [&msgs](proto::MessagePtr msg)
  {
    msgs.push_back(msg);
    return Status::Ok();
  };

  std::vector<ReadState> wrs;
  wrs.push_back(ReadState{.index = 1, .requestCtx = str_to_vector("somedata")});
  MemoryStoragePtr s(new MemoryStorage());

  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, s);

  std::vector<PeerContext> peer{PeerContext{.id = 1}};
  NodeImpl NodeImpl(c, peer);

  NodeImpl.raft_->readStates_ = wrs;
  // ensure the ReadStates can be read out

  ASSERT_TRUE(NodeImpl.HasReady());
  auto rd = NodeImpl.GetReady();

  ASSERT_TRUE(read_state_cmp(rd->readStates, wrs));

  s->Append(rd->entries);
  NodeImpl.Advance(rd);
  // ensure raft.readStates is reset after advance
  ASSERT_TRUE(NodeImpl.raft_->readStates_.empty());

  auto wrequestCtx = str_to_vector("somedata2");
  NodeImpl.Campaign();
  while (true)
  {
    rd = NodeImpl.GetReady();
    s->Append(rd->entries);
    if (rd->softState->lead == NodeImpl.raft_->id_)
    {
      NodeImpl.Advance(rd);

      // Once we are the leader, issue a ReadIndex request
      NodeImpl.raft_->step_ = appendStep;
      NodeImpl.ReadIndex(wrequestCtx);
      break;
    }
    NodeImpl.Advance(rd);
  }
  // ensure that MsgReadIndex message is sent to the underlying raft
  ASSERT_TRUE(msgs.size() == 1);
  ASSERT_TRUE(msgs[0]->type == proto::MsgReadIndex);
  ASSERT_TRUE(msgs[0]->entries[0].data == wrequestCtx);
}

TEST(test_NodeImpl, NodeImplStart)
{
  proto::ConfChange cc;
  cc.confChangeType = proto::ConfChangeAddNode;
  cc.nodeId = 1;
  cc.id = 0;
  auto ccdata = cc.Serialize();

  std::vector<ReadyPtr> wants;

  {
    ReadyPtr rd(new Ready());

    rd->hardState.term = 1;
    rd->hardState.commit = 1;
    rd->hardState.vote = 0;

    proto::EntryPtr entry(new proto::Entry());
    entry->term = 1;
    entry->index = 1;
    entry->data = ccdata;
    entry->type = proto::EntryConfChange;
    rd->entries.push_back(entry);

    proto::EntryPtr e1(new proto::Entry());
    // copy
    *e1 = *entry;
    rd->committedEntries.push_back(e1);
    rd->mustSync = true;
    wants.push_back(rd);
  }

  {
    ReadyPtr rd(new Ready());

    rd->hardState.term = 2;
    rd->hardState.commit = 3;
    rd->hardState.vote = 1;

    proto::EntryPtr entry(new proto::Entry());
    entry->term = 2;
    entry->index = 3;
    entry->data = str_to_vector("foo");
    rd->entries.push_back(entry);

    proto::EntryPtr e1(new proto::Entry());
    // copy
    *e1 = *entry;
    rd->committedEntries.push_back(e1);
    rd->mustSync = true;
    wants.push_back(rd);
  }

  MemoryStoragePtr storage(new MemoryStorage());

  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, storage);

  std::vector<PeerContext> peer{PeerContext{.id = 1}};
  NodeImpl NodeImpl(c, peer);

  auto rd = NodeImpl.GetReady();
  if (!rd->Equal(*wants[0]))
  {
    ASSERT_TRUE(rd->Equal(*wants[0]));
  }

  storage->Append(rd->entries);
  NodeImpl.Advance(rd);

  storage->Append(rd->entries);
  NodeImpl.Advance(rd);

  NodeImpl.Campaign();

  rd = NodeImpl.GetReady();
  storage->Append(rd->entries);
  NodeImpl.Advance(rd);

  NodeImpl.Propose(str_to_vector("foo"));
  rd = NodeImpl.GetReady();
  ASSERT_TRUE(rd->Equal(*wants[1]));
  storage->Append(rd->entries);
  NodeImpl.Advance(rd);

  ASSERT_TRUE(!NodeImpl.HasReady());
}

TEST(test_NodeImpl, NodeImplRestart)
{
  std::vector<proto::EntryPtr> entries;
  {
    proto::EntryPtr e1(new proto::Entry());
    e1->term = 1;
    e1->index = 1;
    entries.push_back(e1);

    proto::EntryPtr e2(new proto::Entry());
    e2->term = 1;
    e2->index = 2;
    e2->data = str_to_vector("foo");
  }
  proto::HardState st;
  st.term = 1;
  st.commit = 1;

  ReadyPtr want(new Ready());
  want->committedEntries.push_back(entries[0]);
  want->mustSync = true;

  MemoryStoragePtr storage(new MemoryStorage());
  storage->SetHardState(st);
  storage->Append(entries);

  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, storage);
  std::vector<PeerContext> peer;
  NodeImpl NodeImpl(c, peer);

  auto rd = NodeImpl.GetReady();
  ASSERT_TRUE(rd->Equal(*want));
  NodeImpl.Advance(rd);
  ASSERT_TRUE(!NodeImpl.HasReady());
}

TEST(test_NodeImpl, NodeImplRestartFromSnapshot)
{
  proto::SnapshotPtr snap(new proto::Snapshot());
  snap->metadata.index = 2;
  snap->metadata.term = 1;
  snap->metadata.confState.nodes.push_back(1);
  snap->metadata.confState.nodes.push_back(2);

  std::vector<proto::EntryPtr> entries;
  {
    proto::EntryPtr e1(new proto::Entry());
    e1->term = 1;
    e1->index = 3;
    e1->data = str_to_vector("foo");
    entries.push_back(e1);
  }

  proto::HardState st;
  st.term = 1;
  st.commit = 3;

  ReadyPtr want(new Ready());
  want->committedEntries = entries;
  want->mustSync = true;

  MemoryStoragePtr storage(new MemoryStorage());
  storage->SetHardState(st);
  storage->ApplySnapshot(*snap);
  storage->Append(entries);

  auto c = newTestConfig(1, std::vector<uint64_t>(), 10, 1, storage);
  std::vector<PeerContext> peer;
  NodeImpl NodeImpl(c, peer);

  auto rd = NodeImpl.GetReady();
  ASSERT_TRUE(rd->Equal(*want));
  NodeImpl.Advance(rd);
  ASSERT_TRUE(!NodeImpl.HasReady());
}

TEST(test_NodeImpl, NodeImplCommitPaginationAfterRestart)
{
  MemoryStoragePtr storage(new MemoryStorage());

  proto::HardState persistedHardState;
  persistedHardState.term = 1;
  persistedHardState.vote = 1;
  persistedHardState.commit = 10;

  storage->hardState_ = persistedHardState;
  storage->entries_.clear();

  uint64_t size = 0;
  for (int i = 0; i < 10; ++i)
  {
    proto::EntryPtr entry(new proto::Entry());

    entry->term = 1;
    entry->index = i + 1;
    entry->type = proto::EntryNormal;
    entry->data.push_back('a');

    storage->entries_.push_back(entry);
    size += entry->SerializeSize();
  }

  auto cfg = newTestConfig(1, std::vector<uint64_t>{1}, 10, 1, storage);
  // Set a MaxSizePerMsg that would suggest to Raft that the last committed entry should
  // not be included in the initial rd.CommittedEntries. However, our storage will ignore
  // this and *will* return it (which is how the Commit index ended up being 10 initially).
  cfg.maxSizePerMsg = size - storage->entries_.back()->SerializeSize() - 1;

  {
    proto::EntryPtr entry(new proto::Entry());
    entry->term = 1;
    entry->index = 11;
    entry->type = proto::EntryNormal;
    entry->data = str_to_vector("boom");
    storage->entries_.push_back(entry);
  }

  std::vector<PeerContext> peer;
  peer.push_back(PeerContext{.id = 1});
  NodeImpl NodeImpl(cfg, peer);

  for (uint64_t highestApplied = 0; highestApplied != 11;)
  {
    auto rd = NodeImpl.GetReady();
    size_t n = rd->committedEntries.size();
    ASSERT_TRUE(n != 0);
    uint64_t next = rd->committedEntries[0]->index;
    if (highestApplied != 0 && highestApplied + 1 != next)
    {
      ASSERT_TRUE(false);
    }

    highestApplied = rd->committedEntries[n - 1]->index;
    NodeImpl.Advance(rd);

    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgHeartbeat;
    msg->to = 1;
    msg->from = 1; // illegal, but we get away with it
    msg->from = 1;
    msg->commit = 11;
    NodeImpl.Step(msg);
  }
}

TEST(test_NodeImpl, NodeImplBoundedLogGrowthWithPartition)
{
  uint64_t maxEntries = 16;
  auto data = str_to_vector("testdata");
  proto::EntryPtr entry(new proto::Entry());
  entry->data = data;

  uint64_t maxEntrySize = maxEntries * entry->PayloadSize();
  MemoryStoragePtr s(new MemoryStorage());

  auto cfg = newTestConfig(1, std::vector<uint64_t>{1}, 10, 1, s);
  cfg.maxUncommittedEntriesSize = maxEntrySize;

  std::vector<PeerContext> peer;
  peer.push_back(PeerContext{.id = 1});
  NodeImpl NodeImpl(cfg, peer);
  auto rd = NodeImpl.GetReady();
  s->Append(rd->entries);
  NodeImpl.Advance(rd);

  // Become the leader.
  NodeImpl.Campaign();

  while (true)
  {
    rd = NodeImpl.GetReady();
    s->Append(rd->entries);
    if (rd->softState->lead == NodeImpl.raft_->id_)
    {
      NodeImpl.Advance(rd);
      break;
    }
    NodeImpl.Advance(rd);
  }

  // Simulate a network partition while we make our proposals by never
  // committing anything. These proposals should not cause the leader's
  // log to grow indefinitely.
  for (size_t i = 0; i < 1024; i++)
  {
    NodeImpl.Propose(data);
  }

  // Check the size of leader's uncommitted log tail. It should not exceed the
  // MaxUncommittedEntriesSize limit.
  auto checkUncommitted = [&NodeImpl](uint64_t exp)
  {
    ASSERT_TRUE(NodeImpl.raft_->uncommittedSize_ == exp);
  };
  checkUncommitted(maxEntrySize);

  // Recover from the partition. The uncommitted tail of the Raft log should
  // disappear as entries are committed.
  rd = NodeImpl.GetReady();
  ASSERT_TRUE(rd->committedEntries.size() == maxEntries);
  s->Append(rd->entries);
  NodeImpl.Advance(rd);
  checkUncommitted(0);
}

int main(int argc, char *argv[])
{
  // testing::GTEST_FLAG(filter) = "raft.OldMessages";
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
