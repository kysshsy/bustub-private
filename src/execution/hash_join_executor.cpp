//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  // build
  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    auto value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
    ht_[value].push_back(tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // probe
  static Tuple t_tuple;
  static RID t_rid;
  if (slot_.GetTypeId() != INVALID &&ht_.find(slot_) != ht_.end() && idx_ < ht_[slot_].size()) {
    if (tuple) {
      auto &o_tuple = ht_[slot_][idx_++];
      *tuple = GetJoinTuple(&o_tuple, left_child_->GetOutputSchema(), &t_tuple, right_child_->GetOutputSchema()); 
    }
    if (rid) {
      *rid = t_rid;
    }
    return true;
  }

  Tuple i_tuple;
  RID i_rid;
  while (right_child_->Next(&i_tuple, &i_rid)) {
    auto i_value = plan_->RightJoinKeyExpression()->Evaluate(&i_tuple, right_child_->GetOutputSchema());
    if (ht_.find(i_value) == ht_.end()) {
      continue;
    }
    t_tuple = i_tuple;
    t_rid = i_rid;
    slot_ = i_value;
    idx_ = 1; // the first time use constant, next is 1

    if (tuple) {
      Tuple o_tuple = ht_[i_value][0];
      *tuple = GetJoinTuple(&o_tuple, left_child_->GetOutputSchema(), &i_tuple, right_child_->GetOutputSchema());
    }
    if (rid) {
      *rid = i_rid;
    }
    return true;
  }
  return false;
}
Tuple HashJoinExecutor::GetJoinTuple(Tuple *o_tuple, const Schema *o_schema, Tuple *i_tuple, const Schema *i_schema) {
  auto out_schema = GetOutputSchema();

  std::vector<Value> vals;
  for (auto &col : out_schema->GetColumns()) {
    vals.emplace_back(col.GetExpr()->EvaluateJoin(o_tuple, o_schema, i_tuple, i_schema));
  }
  return Tuple(vals, out_schema);
}

}  // namespace bustub
