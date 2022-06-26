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
        std::vector<Value> vals;
        for (const auto &c : plan_->OutputSchema()->GetColumns()) {
          vals.emplace_back(c.GetExpr()->EvaluateJoin(&left_tuple_, l_schema, &right_tuple, r_schema));
        }
        *tuple = Tuple(vals, plan_->OutputSchema());
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
