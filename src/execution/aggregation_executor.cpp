//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  assert(child_);
  child_->Init();
  Tuple c_tuple;
  while (child_->Next(&c_tuple, nullptr)) {
    aht_.InsertCombine(MakeAggregateKey(&c_tuple), MakeAggregateValue(&c_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr &&
        !plan_->GetHaving()->EvaluateAggregate(value.aggregates_, value.aggregates_).GetAs<bool>()) {
      continue;
    }
    if (tuple != nullptr) {
      // TODO(me): group by 数据  agg 数据 如何转化成最终的 schema  
      std::vector<Value> ret;
      for (auto &col : plan_->OutputSchema()->GetColumns()) {
        ret.emplace_back(col.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
      }
      *tuple = Tuple(ret, plan_->OutputSchema());
    }
    return true;
  }

  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
