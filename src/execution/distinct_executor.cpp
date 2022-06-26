//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple c_tuple;
    RID c_rid;
    bool res;
    while ((res = child_executor_->Next(&c_tuple, &c_rid))) {
      auto distinct = MakeDistinctValue(c_tuple);
      if (set_.find(distinct) != set_.end()) {
        continue;
      }
      set_.insert(distinct);
      break;
    }    
    if (!res) {
        return false;
    }

    if (tuple) {
      *tuple = c_tuple;
    } 
    if (rid) {
      *rid = c_rid;
    }
    
    return true;
}

}  // namespace bustub
