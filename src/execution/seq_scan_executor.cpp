//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      it_(table_info_->table_->Begin(exec_ctx_->GetTransaction())) {
  schema_ = plan_->OutputSchema();
  predicate_ = plan_->GetPredicate();
}

void SeqScanExecutor::Init() { it_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  TableHeap *th = table_info_->table_.get();
  while (it_ != th->End()) {
    if (predicate_ == nullptr || predicate_->Evaluate(&(*it_), schema_).GetAs<bool>()) {
      if (tuple != nullptr) {
        std::vector<Value> vals;
        auto &o_schema = plan_->OutputSchema()->GetColumns();
        auto &t_schema = table_info_->schema_;
        for (auto &c : o_schema) {
          vals.push_back(c.GetExpr()->Evaluate(&(*it_), &t_schema));
        } 
        *tuple = Tuple(vals, plan_->OutputSchema());
      }
      if (rid != nullptr) {
        *rid = it_->GetRid();
      }
      ++it_;
      return true;
    }
    ++it_;
  }
  return false;
}

}  // namespace bustub
