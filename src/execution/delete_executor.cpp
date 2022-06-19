//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple scan_tuple;
  RID scan_rid;
  while (child_executor_->Next(&scan_tuple, &scan_rid)) {
    table_info->table_->MarkDelete(scan_rid, exec_ctx_->GetTransaction());
    for (auto &index : indexes) {
      Tuple key_tuple = scan_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, scan_rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
