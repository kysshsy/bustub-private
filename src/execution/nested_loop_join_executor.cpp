//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  if (left_executor_) {
    left_executor_->Init();
    left_executor_->Next(&left_tuple_, &left_rid_);
  }
  if (right_executor_) {
    right_executor_->Init();
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (left_rid_ == RID()) {
    return false;
  }
  while (true) {
    Tuple right_tuple;
    RID right_rid;
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        left_rid_ = RID();
        break;
      }
      right_executor_->Init();
      if (!right_executor_->Next(&right_tuple, &right_rid)) {
        left_rid_ = RID();
        break;
      }
    }
    auto l_schema = left_executor_->GetOutputSchema();
    auto r_schema = right_executor_->GetOutputSchema();
    if (plan_->Predicate()->EvaluateJoin(&left_tuple_, l_schema, &right_tuple, r_schema).GetAs<bool>()) {
      if (tuple != nullptr) {
        // join tuple 按合适的schema 输出
        auto l_columns = l_schema->GetColumnCount();
        auto r_columns = r_schema->GetColumnCount();
        std::vector<Value> values(l_columns + r_columns);
        // TODO(unknown): left tuple可以预先取出
        size_t i;
        for (i = 0; i < l_columns; i++) {
          values[i] = left_tuple_.GetValue(l_schema, i);
        }
        for (size_t j = 0; j < r_columns; j++) {
          values[i + j] = right_tuple.GetValue(r_schema, j);
        }
        *tuple = Tuple(values, plan_->OutputSchema());
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
