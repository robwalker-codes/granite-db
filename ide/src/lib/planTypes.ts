export interface PhysicalPlanAggregate {
  fn: string;
  expr: string;
  alias?: string;
}

export interface PhysicalPlanOrder {
  expr: string;
  dir: string;
}

export interface PhysicalPlanProps {
  table?: string;
  index?: string;
  predicate?: string;
  orderBy?: PhysicalPlanOrder[];
  limit?: number;
  offset?: number;
  groupKeys?: string[];
  aggs?: PhysicalPlanAggregate[];
  joinType?: string;
  condition?: string;
  usingIndexOrder?: boolean;
  estimatedRows?: number;
  buildSide?: string;
}

export interface PhysicalPlanNode {
  node: string;
  props?: PhysicalPlanProps;
  children?: PhysicalPlanNode[];
}

export interface ExplainPayload {
  version: number;
  text: string;
  physical?: PhysicalPlanNode;
}
