import { useMemo, useState } from "react";
import type { ExplainPayload, PhysicalPlanNode, PhysicalPlanProps } from "../lib/planTypes";

interface PlanViewProps {
  plan: ExplainPayload | null;
  active: boolean;
}

export default function PlanView({ plan, active }: PlanViewProps) {
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());

  const toggle = (path: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  if (!plan) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-slate-500">
        Use EXPLAIN (Ctrl+L) to see the plan.
      </div>
    );
  }

  if (!active) {
    return null;
  }

  return (
    <div className="flex h-full flex-col overflow-auto bg-slate-50 p-4 dark:bg-slate-900">
      <div className="mb-4 rounded-md border border-slate-300 bg-white p-4 text-sm text-slate-700 shadow-sm dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-300">Plan Text</h2>
        <pre className="whitespace-pre-wrap text-xs leading-5">{plan.text}</pre>
      </div>
      {plan.physical ? (
        <PlanNode node={plan.physical} path="root" collapsed={collapsed} onToggle={toggle} depth={0} />
      ) : (
        <p className="text-sm text-slate-500">No physical plan available.</p>
      )}
    </div>
  );
}

interface PlanNodeProps {
  node: PhysicalPlanNode;
  path: string;
  depth: number;
  collapsed: Set<string>;
  onToggle(path: string): void;
}

function PlanNode({ node, path, depth, collapsed, onToggle }: PlanNodeProps) {
  const isCollapsed = collapsed.has(path);
  const childPathPrefix = useMemo(() => `${path}.`, [path]);
  const details = formatProps(node.props);

  return (
    <div className="mb-3">
      <div className="flex items-start gap-3">
        <button
          type="button"
          onClick={() => onToggle(path)}
          className="mt-1 rounded border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-slate-700 shadow-sm transition hover:bg-slate-100 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100 dark:hover:bg-slate-700"
        >
          {isCollapsed ? "Show" : "Hide"}
        </button>
        <div className="flex-1 rounded-md border border-slate-300 bg-white p-3 shadow-sm dark:border-slate-700 dark:bg-slate-800">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold text-slate-800 dark:text-slate-100">{node.node}</h3>
            <span className="text-xs uppercase tracking-wide text-slate-400">Depth {depth}</span>
          </div>
          {details.length > 0 && (
            <dl className="mt-2 grid grid-cols-1 gap-1 text-xs text-slate-600 dark:text-slate-300">
              {details.map(([label, value]) => (
                <div key={label} className="flex justify-between gap-2">
                  <dt className="font-semibold text-slate-500 dark:text-slate-200">{label}</dt>
                  <dd className="text-right text-slate-700 dark:text-slate-100">{value}</dd>
                </div>
              ))}
            </dl>
          )}
        </div>
      </div>
      {!isCollapsed && node.children && node.children.length > 0 && (
        <div className="ml-6 mt-2 border-l border-dashed border-slate-300 pl-6 dark:border-slate-700">
          {node.children.map((child, index) => (
            <PlanNode
              key={`${childPathPrefix}${index}`}
              node={child}
              path={`${childPathPrefix}${index}`}
              depth={depth + 1}
              collapsed={collapsed}
              onToggle={onToggle}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function formatProps(props?: PhysicalPlanProps): [string, string][] {
  if (!props) {
    return [];
  }
  const entries: [string, string][] = [];
  if (props.table) {
    entries.push(["Table", props.table]);
  }
  if (props.index) {
    entries.push(["Index", props.index]);
  }
  if (props.predicate) {
    entries.push(["Predicate", props.predicate]);
  }
  if (props.condition) {
    entries.push(["Condition", props.condition]);
  }
  if (props.joinType) {
    entries.push(["Join Type", props.joinType]);
  }
  if (props.groupKeys && props.groupKeys.length > 0) {
    entries.push(["Group Keys", props.groupKeys.join(", ")]);
  }
  if (props.aggs && props.aggs.length > 0) {
    entries.push([
      "Aggregates",
      props.aggs
        .map((agg) => `${agg.fn}(${agg.expr})${agg.alias ? ` AS ${agg.alias}` : ""}`)
        .join(", ")
    ]);
  }
  if (props.orderBy && props.orderBy.length > 0) {
    entries.push(["Order By", props.orderBy.map((order) => `${order.expr} ${order.dir}`).join(", ")]);
  }
  if (typeof props.limit === "number") {
    entries.push(["Limit", props.limit.toString()]);
  }
  if (typeof props.offset === "number") {
    entries.push(["Offset", props.offset.toString()]);
  }
  if (typeof props.usingIndexOrder === "boolean") {
    entries.push(["Using Index Order", props.usingIndexOrder ? "Yes" : "No"]);
  }
  if (typeof props.estimatedRows === "number") {
    entries.push(["Estimated Rows", props.estimatedRows.toString()]);
  }
  if (props.buildSide) {
    entries.push(["Build Side", props.buildSide]);
  }
  return entries;
}
