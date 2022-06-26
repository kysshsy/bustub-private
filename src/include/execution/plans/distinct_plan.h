//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/plans/abstract_plan.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
struct DistinctValue {
  std::vector<Value> rows_;

  bool operator==(const DistinctValue &rows) const {
    for (uint32_t i = 0; i < rows_.size(); i++) {
      if (rows_[i].CompareEquals(rows.rows_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};


class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Distinct; }

  /** @return The child plan node */
  const AbstractPlanNode *GetChildPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

}  // namespace bustub


namespace std {

/** Implements std::hash on DistinctValue */
template <>
struct hash<bustub::DistinctValue> {
  std::size_t operator()(const bustub::DistinctValue &dist_key) const {
    size_t curr_hash = 0;
    for (const auto &key : dist_key.rows_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
