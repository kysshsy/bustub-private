//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  if (!plan_->IsRawInsert()) {
    RID insert_rid;
    Tuple scan_tuple;
    while (child_executor_->Next(&scan_tuple, nullptr)) {
      // scan 到的tuple 向 目标表的tuple转换 ? 没有吗
      if (table->table_->InsertTuple(scan_tuple, &insert_rid, exec_ctx_->GetTransaction())) {
        for (auto &index : indexes) {
          index->index_->InsertEntry(
              scan_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()), insert_rid,
              exec_ctx_->GetTransaction());
        }
      }
    }
  } else {
    for (const auto &rv : plan_->RawValues()) {
      RID insert_rid;
      Tuple insert_tuple = Tuple{rv, &(table->schema_)};
      if (table->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction())) {
        for (auto &index : indexes) {
          index->index_->InsertEntry(
              insert_tuple.KeyFromTuple(table->schema_, index->key_schema_, index->index_->GetKeyAttrs()), insert_rid,
              exec_ctx_->GetTransaction());
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
