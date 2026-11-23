// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/client/error.h"
#include "yb/client/session.h"
#include "yb/client/snapshot_test_util.h"
#include "yb/client/table.h"
#include "yb/client/transaction.h"
#include "yb/client/yb_table_name.h"

#include "yb/common/transaction_error.h"
#include "yb/common/wire_protocol.h"

#include "yb/master/master_backup.proxy.h"

#include "yb/rpc/rpc_controller.h"

#include "yb/rocksdb/db.h"

#include "yb/tablet/tablet.h"
#include "yb/tablet/tablet_peer.h"
#include "yb/tablet/tablet_retention_policy.h"
#include "yb/tablet/tablet_snapshots.h"
#include "yb/tablet/transaction_participant.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/test_thread_holder.h"

using namespace std::literals;
using yb::master::SysSnapshotEntryPB;

DECLARE_bool(enable_history_cutoff_propagation);
DECLARE_bool(enable_load_balancing);
DECLARE_bool(flush_rocksdb_on_shutdown);
DECLARE_int32(TEST_inject_status_resolver_complete_delay_ms);
DECLARE_int32(history_cutoff_propagation_interval_ms);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(timestamp_history_retention_interval_sec);
DECLARE_int32(timestamp_syscatalog_history_retention_interval_sec);
DECLARE_int32(txn_max_apply_batch_records);
DECLARE_int32(unresponsive_ts_rpc_timeout_ms);
DECLARE_uint64(max_clock_skew_usec);
DECLARE_uint64(snapshot_coordinator_cleanup_delay_ms);
DECLARE_uint64(snapshot_coordinator_poll_interval_ms);
DECLARE_bool(allow_encryption_at_rest);
DECLARE_bool(TEST_disable_apply_committed_transactions);
DECLARE_bool(TEST_pause_restore_after_ddl_verification_check);
DECLARE_int64(max_concurrent_snapshot_rpcs);
DECLARE_uint64(transaction_resend_applying_interval_usec);

namespace yb {
namespace client {

using ImportedSnapshotData = google::protobuf::RepeatedPtrField<
    master::ImportSnapshotMetaResponsePB::TableMetaPB>;

YB_STRONGLY_TYPED_BOOL(ShutdownFollower);
YB_STRONGLY_TYPED_BOOL(SplitWhileSnapshotWaits);

class BackupTxnTest : public TransactionTestBase<MiniCluster> {
 protected:
  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_history_cutoff_propagation) = true;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_allow_encryption_at_rest) = false;
    SetIsolationLevel(IsolationLevel::SNAPSHOT_ISOLATION);
    mini_cluster_opt_.num_masters = 3;
    TransactionTestBase::SetUp();
    snapshot_util_ = std::make_unique<SnapshotTestUtil>();
    snapshot_util_->SetProxy(&client_->proxy_cache());
    snapshot_util_->SetCluster(cluster_.get());
  }

  void DoBeforeTearDown() override {
    if (!testing::Test::HasFailure()) {
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_flush_rocksdb_on_shutdown) = false;
      ASSERT_OK(cluster_->RestartSync());
    }

    TransactionTestBase::DoBeforeTearDown();
  }

  Status WaitAllSnapshotsDeleted() {
    RETURN_NOT_OK(snapshot_util_->WaitAllSnapshotsDeleted());
    // Check if deleted in DocDB.
    return WaitFor([this]() -> Result<bool> {
      auto peers = ListTabletPeers(cluster_.get(), ListPeersFilter::kAll);
      for (const auto& peer : peers) {
        auto db = VERIFY_RESULT(peer->shared_tablet())->regular_db();
        if (!db) {
          continue;
        }
        auto dir = tablet::TabletSnapshots::SnapshotsDirName(db->GetName());
        auto children = VERIFY_RESULT(Env::Default()->GetChildren(dir, ExcludeDots::kTrue));
        if (!children.empty()) {
          LOG(INFO) << peer->LogPrefix() << "Children: " << AsString(children);
          return false;
        }
      }
      return true;
    }, kWaitTimeout * kTimeMultiplier, "Delete on tablets");
  }

  Result<bool> IsSnapshotImportDone(const ImportedSnapshotData& data) {
    for (const auto& table : data) {
      RETURN_NOT_OK(client_->OpenTable(table.table_ids().new_id()));
    }

    return true;
  }

  void TestDeleteTable(bool restart_masters);
  void TestDeleteSnapshot(bool compact_and_restart);
  void TestLargeTransactionApplyingOnFollower(ShutdownFollower shutdown_follower);
  void TestSnapshotOvertakenBySplit(SplitWhileSnapshotWaits split);

  // SnapshotTestUtil::RestoreSnapshotSchedule uses a 60 s RPC timeout, and the master waits within
  // that RPC for the snapshot it creates for the restore.
  Status RestoreScheduleWithLongTimeout(
      const SnapshotScheduleId& schedule_id, HybridTime restore_at) {
    master::RestoreSnapshotScheduleRequestPB req;
    master::RestoreSnapshotScheduleResponsePB resp;
    rpc::RpcController controller;
    controller.set_timeout(300s * kTimeMultiplier);
    req.set_snapshot_schedule_id(schedule_id.data(), schedule_id.size());
    req.set_restore_ht(restore_at.ToUint64());
    RETURN_NOT_OK(VERIFY_RESULT(snapshot_util_->MakeBackupServiceProxy())
        .RestoreSnapshotSchedule(req, &resp, &controller));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    auto restoration_id = VERIFY_RESULT(
        FullyDecodeTxnSnapshotRestorationId(resp.restoration_id()));
    return snapshot_util_->WaitRestorationInState(
        restoration_id, master::SysSnapshotEntryPB::RESTORED, 120s * kTimeMultiplier);
  }

  std::unique_ptr<SnapshotTestUtil> snapshot_util_;
};

TEST_F(BackupTxnTest, Simple) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_max_clock_skew_usec) =
      std::chrono::duration_cast<std::chrono::microseconds>(1s).count() * kTimeMultiplier;
  ASSERT_NO_FATALS(WriteData());

  TxnSnapshotId snapshot_id = ASSERT_RESULT(snapshot_util_->StartSnapshot(table_));

  bool has_pending = false;
  ASSERT_OK(WaitFor([this, &snapshot_id, &has_pending]() -> Result<bool> {
    if (!VERIFY_RESULT(snapshot_util_->IsSnapshotDone(snapshot_id))) {
      has_pending = true;
      return false;
    }
    return true;
  }, 10s, "Snapshot done"));

  ASSERT_TRUE(has_pending);

  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  ASSERT_NO_FATALS(WriteData(WriteOpType::UPDATE));
  ASSERT_NO_FATALS(VerifyData(1, WriteOpType::UPDATE));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id));

  ASSERT_NO_FATALS(VerifyData(/* num_transactions=*/ 1, WriteOpType::INSERT));
}

TEST_F(BackupTxnTest, PointInTimeRestore) {
  ASSERT_NO_FATALS(WriteData());
  auto hybrid_time = cluster_->mini_tablet_server(0)->server()->Clock()->Now();
  ASSERT_NO_FATALS(WriteData(WriteOpType::UPDATE));

  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id, hybrid_time));

  ASSERT_NO_FATALS(VerifyData(/* num_transactions=*/ 1, WriteOpType::INSERT));
}

// This test writes a lot of update to the same key.
// Then takes snapshot and restores it to time before the write.
// So we test how filtering iterator works in case where a lot of record should be skipped.
TEST_F(BackupTxnTest, PointInTimeBigSkipRestore) {
  constexpr int kNumWrites = RegularBuildVsSanitizers(100000, 100);
  constexpr int kKey = 123;

  std::vector<std::future<FlushStatus>> futures;
  auto session = CreateSession();
  ASSERT_OK(WriteRow(session, kKey, 0));
  auto hybrid_time = cluster_->mini_tablet_server(0)->server()->Clock()->Now();
  for (int r = 1; r <= kNumWrites; ++r) {
    ASSERT_OK(WriteRow(session, kKey, r, WriteOpType::INSERT, client::Flush::kFalse));
    futures.push_back(session->FlushFuture());
  }

  int good = 0;
  for (auto& future : futures) {
    if (future.get().status.ok()) {
      ++good;
    }
  }

  LOG(INFO) << "Total good: " << good;

  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id, hybrid_time));

  auto value = ASSERT_RESULT(SelectRow(session, kKey));
  ASSERT_EQ(value, 0);
}

// Restore to the time before current history cutoff.
TEST_F(BackupTxnTest, PointInTimeRestoreBeforeHistoryCutoff) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_history_cutoff_propagation_interval_ms) = 1;

  ASSERT_NO_FATALS(WriteData());
  auto hybrid_time = cluster_->mini_tablet_server(0)->server()->Clock()->Now();
  ASSERT_NO_FATALS(WriteData(WriteOpType::UPDATE));

  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_history_retention_interval_sec) = 0;

  ASSERT_OK(WaitFor([this, hybrid_time]() -> Result<bool> {
    auto peers = ListTabletPeers(cluster_.get(), ListPeersFilter::kLeaders);
    for (const auto& peer : peers) {
      // History cutoff is not moved for tablets w/o writes after current history cutoff.
      // So just ignore such tablets.
      auto tablet = peer->shared_tablet_maybe_null();
      if (!tablet) {
        continue;
      }
      if (tablet->mvcc_manager()->LastReplicatedHybridTime() < hybrid_time) {
        continue;
      }
      // Check that history cutoff is after hybrid_time.
      auto read_operation = tablet::ScopedReadOperation::Create(
          tablet.get(), tablet::RequireLease::kTrue, ReadHybridTime::SingleTime(hybrid_time));
      if (read_operation.ok()) {
        auto policy = tablet->RetentionPolicy();
        LOG(INFO) << "Pending history cutoff, tablet: " << peer->tablet_id()
                  << ", current: "
                  << policy->GetRetentionDirective().history_cutoff
                                                    .primary_cutoff_ht
                  << ", desired: " << hybrid_time;
        return false;
      }
      if (!read_operation.status().IsSnapshotTooOld()) {
        return read_operation.status();
      }
    }
    return true;
  }, kWaitTimeout, "History retention move past hybrid time"));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id, hybrid_time));

  ASSERT_NO_FATALS(VerifyData(/* num_transactions=*/ 1, WriteOpType::INSERT));
}

TEST_F(BackupTxnTest, Persistence) {
  LOG(INFO) << "Write data";

  ASSERT_NO_FATALS(WriteData());

  LOG(INFO) << "Create snapshot";

  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));

  LOG(INFO) << "First restart";

  ASSERT_OK(ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->Restart());
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  LOG(INFO) << "Create namespace";

  // Create namespace and flush, to avoid replaying logs in the master tablet containing the
  // CREATE_ON_MASTER operation for the snapshot.
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kTableName.namespace_name() + "_Test",
                                                kTableName.namespace_type()));

  LOG(INFO) << "Flush";

  auto tablet_peer = ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->tablet_peer();
  auto tablet = ASSERT_RESULT(tablet_peer->shared_tablet());
  ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync, rocksdb::FlushReason::kTestOnly));

  LOG(INFO) << "Second restart";

  ASSERT_OK(ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->Restart());

  LOG(INFO) << "Verify";

  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));
}

void BackupTxnTest::TestDeleteSnapshot(bool compact_and_restart) {
  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));
  ASSERT_OK(snapshot_util_->DeleteSnapshot(snapshot_id));
  if (compact_and_restart) {
    for (size_t i = 0; i != cluster_->num_masters(); ++i) {
      ASSERT_OK(ASSERT_RESULT(cluster_->mini_master(i)->tablet_peer()->shared_tablet())
                    ->ForceManualRocksDBCompact());
    }
    ASSERT_OK(cluster_->RestartSync());
  }
  ASSERT_OK(snapshot_util_->WaitAllSnapshotsDeleted());

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_snapshot_coordinator_cleanup_delay_ms) = 1000;

  ASSERT_OK(snapshot_util_->WaitAllSnapshotsCleaned());
}

TEST_F(BackupTxnTest, Delete) {
  TestDeleteSnapshot(/* compact_and_restart= */ false);
}

TEST_F(BackupTxnTest, DeleteWithCompaction) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_history_cutoff_propagation) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_history_retention_interval_sec) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_syscatalog_history_retention_interval_sec) = 0;

  TestDeleteSnapshot(/* compact_and_restart= */ true);
}

TEST_F(BackupTxnTest, CleanupAfterRestart) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_snapshot_coordinator_cleanup_delay_ms) = 300000;

  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));
  ASSERT_OK(snapshot_util_->DeleteSnapshot(snapshot_id));
  ASSERT_OK(snapshot_util_->WaitAllSnapshotsDeleted());

  ASSERT_FALSE(ASSERT_RESULT(snapshot_util_->ListSnapshots()).empty());

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_snapshot_coordinator_cleanup_delay_ms) = 1000;
  ASSERT_OK(ASSERT_RESULT(cluster_->GetLeaderMiniMaster())->Restart());

  ASSERT_OK(snapshot_util_->WaitAllSnapshotsCleaned());
}

TEST_F(BackupTxnTest, ImportMeta) {
  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::COMPLETE,
                                           table_.table()->GetPartitionCount()));

  auto snapshots = ASSERT_RESULT(snapshot_util_->ListSnapshots(
      snapshot_id, ListDeleted::kFalse, PrepareForBackup::kTrue));
  ASSERT_EQ(snapshots.size(), 1);

  ASSERT_OK(snapshot_util_->DeleteSnapshot(snapshot_id));
  ASSERT_OK(snapshot_util_->WaitAllSnapshotsDeleted());
  ASSERT_OK(client_->DeleteTable(kTableName));
  ASSERT_OK(client_->DeleteNamespace(kTableName.namespace_name()));

  auto import_data = ASSERT_RESULT(snapshot_util_->StartImportSnapshot(snapshots[0]));

  ASSERT_OK(WaitFor([this, import_data] {
    return IsSnapshotImportDone(import_data);
  }, kWaitTimeout * kTimeMultiplier, "Complete import snapshot"));

  ASSERT_OK(table_.Open(kTableName, client_.get()));

  ASSERT_NO_FATALS(WriteData());
}

TEST_F(BackupTxnTest, Retry) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_unresponsive_ts_rpc_timeout_ms) = 1000;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_snapshot_coordinator_poll_interval_ms) = 1000;

  ASSERT_NO_FATALS(WriteData());

  ShutdownAllTServers(cluster_.get());

  TxnSnapshotId snapshot_id = ASSERT_RESULT(snapshot_util_->StartSnapshot(table_));

  std::this_thread::sleep_for(FLAGS_unresponsive_ts_rpc_timeout_ms * 1ms + 1s);

  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::CREATING,
                                           table_.table()->GetPartitionCount()));

  ASSERT_OK(StartAllTServers(cluster_.get()));

  ASSERT_OK(snapshot_util_->WaitSnapshotDone(snapshot_id, 15s));

  ASSERT_NO_FATALS(VerifyData());

  ASSERT_NO_FATALS(WriteData(WriteOpType::UPDATE));
  ASSERT_NO_FATALS(VerifyData(WriteOpType::UPDATE));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id));

  ASSERT_NO_FATALS(VerifyData());
}

TEST_F(BackupTxnTest, Failure) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_history_retention_interval_sec) = 0;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_history_cutoff_propagation_interval_ms) = 1;

  ASSERT_NO_FATALS(WriteData());

  ShutdownAllTServers(cluster_.get());

  TxnSnapshotId snapshot_id = ASSERT_RESULT(snapshot_util_->StartSnapshot(table_));

  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::CREATING,
                                           table_.table()->GetPartitionCount()));

  ShutdownAllMasters(cluster_.get());

  ASSERT_OK(StartAllTServers(cluster_.get()));

  // Wait 2 rounds to be sure that very recent history cutoff committed.
  std::this_thread::sleep_for(FLAGS_raft_heartbeat_interval_ms * 2ms * kTimeMultiplier);

  ASSERT_OK(StartAllMasters(cluster_.get()));

  ASSERT_OK(snapshot_util_->WaitSnapshotInState(snapshot_id, SysSnapshotEntryPB::FAILED, 30s));
}

TEST_F(BackupTxnTest, Restart) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_timestamp_history_retention_interval_sec) =
      std::chrono::duration_cast<std::chrono::seconds>(kWaitTimeout).count() *
      kTimeMultiplier;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_history_cutoff_propagation_interval_ms) = 1;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_flush_rocksdb_on_shutdown) = false;

  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));

  ShutdownAllMasters(cluster_.get());

  // Wait 2 rounds to be sure that very recent history cutoff committed.
  std::this_thread::sleep_for((FLAGS_timestamp_history_retention_interval_sec + 1) * 1s);

  ASSERT_OK(StartAllMasters(cluster_.get()));

  ASSERT_OK(snapshot_util_->WaitSnapshotInState(snapshot_id, SysSnapshotEntryPB::COMPLETE, 1s));
}

TEST_F(BackupTxnTest, CompleteAndBounceMaster) {
  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));

  std::this_thread::sleep_for(1s);

  ASSERT_OK(client_->DeleteTable(kTableName));

  auto leader = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  leader->Shutdown();

  ASSERT_OK(snapshot_util_->WaitSnapshotInState(snapshot_id, SysSnapshotEntryPB::COMPLETE, 1s));

  ASSERT_OK(leader->Start());
}

TEST_F(BackupTxnTest, FlushSysCatalogAndDelete) {
  ASSERT_NO_FATALS(WriteData());
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));

  for (size_t i = 0; i != cluster_->num_masters(); ++i) {
    auto tablet_peer = cluster_->mini_master(i)->tablet_peer();
    auto tablet = ASSERT_RESULT(tablet_peer->shared_tablet());
    ASSERT_OK(tablet->Flush(tablet::FlushMode::kSync, rocksdb::FlushReason::kTestOnly));
  }

  ShutdownAllTServers(cluster_.get());
  ASSERT_OK(snapshot_util_->DeleteSnapshot(snapshot_id));

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_flush_rocksdb_on_shutdown) = false;
  ShutdownAllMasters(cluster_.get());

  LOG(INFO) << "Start masters";

  ASSERT_OK(StartAllMasters(cluster_.get()));
  ASSERT_OK(StartAllTServers(cluster_.get()));

  ASSERT_OK(snapshot_util_->WaitSnapshotInState(snapshot_id, SysSnapshotEntryPB::DELETED, 30s));
}

// Workload writes same value across all keys in a txn, using sevaral txns in concurrently.
// Checks that after restore all keys/tablets report same value.
TEST_F(BackupTxnTest, Consistency) {
  constexpr int kThreads = 5;
  constexpr int kKeys = 10;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_inject_status_resolver_complete_delay_ms) = 100;

  TestThreadHolder thread_holder;
  std::atomic<int> value(0);

  for (int i = 0; i != kThreads; ++i) {
    thread_holder.AddThreadFunctor([this, &stop = thread_holder.stop_flag(), &value] {
      auto session = CreateSession();
      while (!stop.load(std::memory_order_acquire)) {
        auto txn = CreateTransaction();
        session->SetTransaction(txn);
        auto v = value.fetch_add(1, std::memory_order_acq_rel);
        for (int j = 0; j != kKeys; ++j) {
          ASSERT_OK(WriteRow(session, j, v, WriteOpType::INSERT, Flush::kFalse));
        }
        auto status = session->TEST_Flush();
        if (status.ok()) {
          status = txn->CommitFuture().get();
        }
        if (!status.ok()) {
          TransactionError txn_error(status);
          ASSERT_TRUE(txn_error == TransactionErrorCode::kConflict ||
                      txn_error == TransactionErrorCode::kAborted ||
                      status.IsCombined()) << status;
        } else {
          LOG(INFO) << "Committed: " << txn->id() << ", written: " << v;
        }
      }
    });
  }

  while (value.load(std::memory_order_acquire) < 100) {
    std::this_thread::sleep_for(5ms);
  }

  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));

  thread_holder.Stop();

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id));

  auto session = CreateSession();
  int restored_value = -1;
  for (int j = 0; j != kKeys; ++j) {
    auto current_value = ASSERT_RESULT(SelectRow(session, j));
    LOG(INFO) << "Key: " << j << ", value: " << current_value;
    if (restored_value == -1) {
      restored_value = current_value;
    } else {
      ASSERT_EQ(restored_value, current_value);
    }
  }

  LOG(INFO) << "Value: " << restored_value;
}

// A follower creates its snapshot checkpoint when it applies the snapshot operation. Intents of a
// large transaction committed before the snapshot hybrid time could still be written to its
// regular DB by ApplyIntentsTask at that point, and restore drops intents.
void BackupTxnTest::TestLargeTransactionApplyingOnFollower(ShutdownFollower shutdown_follower) {
  constexpr int kNumKeys = 500;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_txn_max_apply_batch_records) = 1;

  ASSERT_OK(WaitForTableLeaders(cluster_.get(), table_->id(), kWaitTimeout * kTimeMultiplier));
  auto followers = ListTableTabletPeers(cluster_.get(), table_->id(), ListPeersFilter::kNonLeaders);
  ASSERT_FALSE(followers.empty());
  for (const auto& peer : followers) {
    ASSERT_RESULT(peer->shared_tablet())->TEST_SleepBeforeApplyIntents(500ms * kTimeMultiplier);
  }

  auto session = CreateSession();
  auto txn = CreateTransaction();
  session->SetTransaction(txn);
  for (int key = 0; key != kNumKeys; ++key) {
    ASSERT_OK(WriteRow(session, key, key, WriteOpType::INSERT, Flush::kFalse));
  }
  ASSERT_OK(session->TEST_Flush());
  ASSERT_OK(txn->CommitFuture().get());

  RegexWaiterLogSink postponed(Format(
      ".*P $0 .*Postpone applying SNAPSHOT_OP.*", followers.front()->permanent_uuid()));
  auto snapshot_id = ASSERT_RESULT(snapshot_util_->CreateSnapshot(table_));
  ASSERT_GT(CountRunningTransactions(), 0) << "Transaction applied before snapshot";
  ASSERT_OK(postponed.WaitFor(kWaitTimeout * kTimeMultiplier));

  if (shutdown_follower) {
    auto uuid = followers.front()->permanent_uuid();
    auto* ts = cluster_->find_tablet_server(uuid);
    auto start = MonoTime::Now();
    ts->Shutdown();
    // Shutdown waits for one delayed batch per tablet, while the rest of the transaction takes a
    // delayed batch per record on each tablet.
    ASSERT_LT(MonoTime::Now() - start, MonoDelta(10s * kTimeMultiplier));
    std::erase_if(followers, [&uuid](const auto& peer) { return peer->permanent_uuid() == uuid; });
    ASSERT_OK(ts->Start());
  }

  for (const auto& peer : followers) {
    ASSERT_RESULT(peer->shared_tablet())->TEST_SleepBeforeApplyIntents(MonoDelta());
  }
  ASSERT_OK(WaitForTableIntentsApplied(cluster_.get(), table_->id()));

  ASSERT_OK(snapshot_util_->RestoreSnapshot(snapshot_id));
  ASSERT_OK(WaitAllReplicasSynchronizedWithLeader(
      cluster_.get(), kWaitTimeout * kTimeMultiplier, RequireLeaderIsReady::kFalse));

  // Each replica restores its own checkpoint.
  std::map<TabletId, std::vector<std::pair<std::string, std::vector<std::string>>>> dumps;
  for (const auto& peer : ListTableTabletPeers(cluster_.get(), table_->id())) {
    std::vector<std::string> dump;
    ASSERT_RESULT(peer->shared_tablet())->TEST_DocDBDumpToContainer(
        dump, docdb::IncludeIntents::kFalse);
    dumps[peer->tablet_id()].emplace_back(peer->permanent_uuid(), std::move(dump));
  }
  for (const auto& [tablet_id, replicas] : dumps) {
    const auto& [first_uuid, first_dump] = replicas.front();
    for (const auto& [uuid, dump] : replicas) {
      ASSERT_TRUE(dump == first_dump)
          << "T " << tablet_id << ": " << dump.size() << " records on " << uuid << " vs "
          << first_dump.size() << " on " << first_uuid;
    }
  }
}

TEST_F(BackupTxnTest, LargeTransactionApplyingOnFollower) {
  TestLargeTransactionApplyingOnFollower(ShutdownFollower::kFalse);
}

// The follower postpones the snapshot operation, so its shutdown does not wait for the apply.
TEST_F(BackupTxnTest, ShutdownFollowerPostponingSnapshot) {
  TestLargeTransactionApplyingOnFollower(ShutdownFollower::kTrue);
}

// The leader of a tablet waits in ResolveIntents, before submitting the snapshot operation, for the
// APPLY of a transaction T committed at or before the snapshot hybrid time. A split of that tablet
// can overtake the snapshot operation:
// 1. The leader waits for T's APPLY. The master re-sends the snapshot RPC when it times out.
// 2. The tablet splits. The coordinator sends T's APPLY to the children, they apply it, and the
//    coordinator removes T.
// 3. A later attempt on the split parent asks the coordinator for T's status. T is unknown there,
//    so the answer is ABORTED: the participant drops T and the snapshot operation goes into the
//    parent's log after the split operation.
// 4. The parent's checkpoint is its frozen data, which never got T's APPLY. Restore drops intents,
//    so the restored copy misses T on the parent's key range.
// Seen in the stress test test_packed_row_compaction_merge_snapshot_split on 2.33.0.0-b75 (a DB
// clone lost a committed transaction on one split parent's range).
// split == kFalse is the control: the same steps without the split.
void BackupTxnTest::TestSnapshotOvertakenBySplit(SplitWhileSnapshotWaits split) {
  constexpr int kNumEarlyRows = 100;
  constexpr int kLateKeyBase = 100000;
  constexpr int kNumLateRows = 100;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_resend_applying_interval_usec) = 1000000;
  // Every tablet gets its snapshot RPC at once. By default one RPC per tserver is outstanding, and
  // a tablet whose leader waits for T holds that slot.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_max_concurrent_snapshot_rpcs) = 0;
  google::SetVLOGLevel("transaction_participant", 4);

  // Under a schedule a split parent is hidden, not deleted, so PITR can restore it.
  // Long interval: the only later snapshot is the one the restore creates.
  auto schedule_id = ASSERT_RESULT(snapshot_util_->CreateSchedule(
      table_, kTableName.namespace_type(), kTableName.namespace_name(), WaitSnapshot::kTrue,
      MonoDelta(1h), MonoDelta(20h)));

  auto write_rows = [this](int key_base, int num_rows) -> Result<TransactionId> {
    auto txn = CreateTransaction();
    auto session = CreateSession(txn);
    for (int i = 0; i != num_rows; ++i) {
      RETURN_NOT_OK(WriteRow(
          session, key_base + i, key_base + i, WriteOpType::INSERT, Flush::kFalse));
    }
    RETURN_NOT_OK(session->TEST_Flush());
    RETURN_NOT_OK(txn->CommitFuture().get());
    return txn->id();
  };
  // Point reads: SelectAllRows uses the table handle's partitions, which a split makes stale.
  auto count_rows = [this](int key_base, int num_rows) -> Result<int> {
    auto session = CreateSession();
    int result = 0;
    for (int i = 0; i != num_rows; ++i) {
      auto value = SelectRow(session, key_base + i);
      if (value.ok()) {
        ++result;
      } else if (!value.status().IsNotFound()) {
        return value.status();
      }
    }
    return result;
  };

  // Rows in regular DB, so the tablet has a split key.
  ASSERT_RESULT(write_rows(0, kNumEarlyRows));
  ASSERT_OK(WaitForTableIntentsApplied(cluster_.get(), table_->id(), kWaitTimeout));
  ASSERT_OK(cluster_->FlushTablets());

  // T commits, but its participants do not get APPLY yet.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_disable_apply_committed_transactions) = true;
  const auto late_txn_id = ASSERT_RESULT(write_rows(kLateKeyBase, kNumLateRows));
  LOG(INFO) << "Late transaction committed: " << late_txn_id;

  // The tablet to split: one with T's intents.
  TabletId split_tablet_id;
  for (const auto& peer : ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id())) {
    auto tablet = ASSERT_RESULT(peer->shared_tablet());
    if (tablet->transaction_participant()->GetNumRunningTransactions() != 0) {
      split_tablet_id = peer->tablet_id();
      break;
    }
  }
  ASSERT_FALSE(split_tablet_id.empty());
  const auto num_tablets = ListTableActiveTabletLeadersPeers(cluster_.get(), table_->id()).size();

  // Restore to a time after T committed. The master creates the snapshot the restore needs, and
  // pauses once that snapshot is complete, before restoring.
  const auto restore_at = cluster_->mini_tablet_server(0)->server()->Clock()->Now();
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_restore_after_ddl_verification_check) = true;
  StringWaiterLogSink restore_paused(
      "Pausing due to flag TEST_pause_restore_after_ddl_verification_check");
  StringWaiterLogSink snapshot_op_started(
      "Processing TabletSnapshotOp for tablet " + split_tablet_id);
  TestThreadHolder thread_holder;
  Status restore_status;
  thread_holder.AddThreadFunctor([this, &schedule_id, restore_at, &restore_status] {
    restore_status = RestoreScheduleWithLongTimeout(schedule_id, restore_at);
  });

  // The leader of the tablet now waits in ResolveIntents for T's APPLY.
  ASSERT_OK(snapshot_op_started.WaitFor(60s * kTimeMultiplier));
  SleepFor(1s * kTimeMultiplier);
  if (split) {
    LOG(INFO) << "Splitting " << split_tablet_id << " while its snapshot operation waits";
    ASSERT_OK(InvokeSplitTabletRpc(cluster_.get(), split_tablet_id));
    ASSERT_OK(ResultToStatus(WaitForTableActiveTabletLeadersPeers(
        cluster_.get(), table_->id(), num_tablets + 1)));
    LOG(INFO) << "Split children have leaders";
  }
  LOG(INFO) << "Enabling APPLY";
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_disable_apply_committed_transactions) = false;

  // The snapshot is complete and the restore has not started: live data has every row of T.
  ASSERT_OK(restore_paused.WaitFor(240s * kTimeMultiplier));
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    return VERIFY_RESULT(count_rows(kLateKeyBase, kNumLateRows)) == kNumLateRows;
  }, 60s * kTimeMultiplier, "Live data has the late transaction"));
  LOG(INFO) << "Live data before the restore: " << kNumLateRows << " of " << kNumLateRows
            << " rows of " << late_txn_id;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_pause_restore_after_ddl_verification_check) = false;
  thread_holder.JoinAll();
  ASSERT_OK(restore_status);

  const auto early_rows = ASSERT_RESULT(count_rows(0, kNumEarlyRows));
  const auto late_rows = ASSERT_RESULT(count_rows(kLateKeyBase, kNumLateRows));
  LOG(INFO) << "Restored: " << early_rows << " of " << kNumEarlyRows << " earlier rows, "
            << late_rows << " of " << kNumLateRows << " rows of " << late_txn_id;
  ASSERT_EQ(early_rows, kNumEarlyRows);
  ASSERT_EQ(late_rows, kNumLateRows)
      << "Restore to a time after the commit lost rows of " << late_txn_id;
}

TEST_F(BackupTxnTest, SnapshotOvertakenBySplit) {
  TestSnapshotOvertakenBySplit(SplitWhileSnapshotWaits::kTrue);
}

// Control: the same without the split.
TEST_F(BackupTxnTest, SnapshotWaitsForApplyWithoutSplit) {
  TestSnapshotOvertakenBySplit(SplitWhileSnapshotWaits::kFalse);
}

void BackupTxnTest::TestDeleteTable(bool restart_masters) {
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_unresponsive_ts_rpc_timeout_ms) = 1000;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_snapshot_coordinator_poll_interval_ms) = 2500 * kTimeMultiplier;

  ASSERT_NO_FATALS(WriteData());

  ShutdownAllTServers(cluster_.get());
  TxnSnapshotId snapshot_id = ASSERT_RESULT(snapshot_util_->StartSnapshot(table_));

  std::this_thread::sleep_for(FLAGS_unresponsive_ts_rpc_timeout_ms * 1ms + 1s);
  ASSERT_OK(snapshot_util_->VerifySnapshot(snapshot_id, SysSnapshotEntryPB::CREATING,
                                           table_.table()->GetPartitionCount()));

  ASSERT_OK(client_->DeleteTable(kTableName, false));

  if (restart_masters) {
    ShutdownAllMasters(cluster_.get());
  }

  ASSERT_OK(StartAllTServers(cluster_.get()));

  if (restart_masters) {
    ASSERT_OK(StartAllMasters(cluster_.get()));
    ASSERT_OK(WaitUntilMasterHasLeader(cluster_.get(), 5s * kTimeMultiplier));
  }

  ASSERT_OK(snapshot_util_->WaitSnapshotInState(snapshot_id, SysSnapshotEntryPB::FAILED,
                                                5s * kTimeMultiplier));
}

TEST_F(BackupTxnTest, DeleteTable) {
  TestDeleteTable(/* restart_masters= */ false);
}

TEST_F(BackupTxnTest, DeleteTableWithMastersRestart) {
  TestDeleteTable(/* restart_masters= */ true);
}

} // namespace client
} // namespace yb
