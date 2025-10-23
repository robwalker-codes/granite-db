package exec

import (
	"fmt"
	"strings"

	"github.com/example/granite-db/engine/internal/sql/parser"
	"github.com/example/granite-db/engine/internal/sql/validator"
)

// PhysicalPlanNode describes an operator in the physical execution tree exposed via EXPLAIN JSON.
type PhysicalPlanNode struct {
	Node     string              `json:"node"`
	Props    *PhysicalPlanProps  `json:"props,omitempty"`
	Children []*PhysicalPlanNode `json:"children,omitempty"`
}

// PhysicalPlanProps captures optional attributes for a physical operator.
type PhysicalPlanProps struct {
	Table           *string                 `json:"table,omitempty"`
	Index           *string                 `json:"index,omitempty"`
	Predicate       *string                 `json:"predicate,omitempty"`
	OrderBy         []PhysicalPlanOrder     `json:"orderBy,omitempty"`
	Limit           *int                    `json:"limit,omitempty"`
	Offset          *int                    `json:"offset,omitempty"`
	GroupKeys       []string                `json:"groupKeys,omitempty"`
	Aggs            []PhysicalPlanAggregate `json:"aggs,omitempty"`
	JoinType        *string                 `json:"joinType,omitempty"`
	Condition       *string                 `json:"condition,omitempty"`
	UsingIndexOrder *bool                   `json:"usingIndexOrder,omitempty"`
}

// PhysicalPlanOrder records the requested ordering for a Sort node.
type PhysicalPlanOrder struct {
	Expr string `json:"expr"`
	Dir  string `json:"dir"`
}

// PhysicalPlanAggregate summarises an aggregate computed by HashAgg.
type PhysicalPlanAggregate struct {
	Fn    string `json:"fn"`
	Expr  string `json:"expr"`
	Alias string `json:"alias,omitempty"`
}

// PhysicalPlan builds the physical execution tree for the supplied statement.
func (e *Executor) PhysicalPlan(stmt parser.Statement) (*PhysicalPlanNode, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return e.buildSelectPhysicalPlan(s)
	default:
		return nil, fmt.Errorf("exec: EXPLAIN JSON supports SELECT statements only")
	}
}

func (e *Executor) buildSelectPhysicalPlan(stmt *parser.SelectStmt) (*PhysicalPlanNode, error) {
	validated, err := validator.ValidateSelect(e.catalog, stmt)
	if err != nil {
		return nil, err
	}
	var idxChoice *indexChoice
	if len(validated.Sources) == 1 && len(validated.Joins) == 0 {
		idxChoice = e.chooseIndex(validated)
	}
	builder := &physicalPlanBuilder{
		stmt:       stmt,
		validated:  validated,
		idxChoice:  idxChoice,
		aggAliases: gatherAggregateAliases(stmt),
	}
	return builder.build(), nil
}

type physicalPlanBuilder struct {
	stmt       *parser.SelectStmt
	validated  *validator.ValidatedSelect
	idxChoice  *indexChoice
	aggAliases map[string]string
}

func (b *physicalPlanBuilder) build() *PhysicalPlanNode {
	input := b.buildFromTree()
	if input == nil {
		input = &PhysicalPlanNode{Node: "Const"}
	}
	if b.validated.Filter != nil && b.validated.FilterText != "" {
		input = newFilterNode(b.validated.FilterText, input)
	}
	if len(b.validated.Groupings) > 0 || len(b.validated.Aggregates) > 0 {
		input = b.wrapAggregation(input)
	}
	if b.validated.Having != nil && b.validated.HavingText != "" {
		input = newFilterNode(b.validated.HavingText, input)
	}
	if len(b.validated.OrderBy) > 0 {
		input = b.wrapSort(input)
	}
	if b.validated.Limit != nil {
		input = wrapLimitNode(b.validated.Limit, input)
	}
	root := &PhysicalPlanNode{Node: "Project"}
	root.Children = append(root.Children, input)
	return root
}

func (b *physicalPlanBuilder) buildFromTree() *PhysicalPlanNode {
	if len(b.validated.Sources) == 0 {
		return nil
	}
	left := b.planForSource(b.validated.Sources[0], b.idxChoice)
	for _, join := range b.validated.Joins {
		node := &PhysicalPlanNode{Node: joinAlgorithm(join)}
		props := &PhysicalPlanProps{}
		jt := formatJoinType(join.Type)
		props.JoinType = &jt
		if join.ConditionText != "" {
			cond := join.ConditionText
			props.Condition = &cond
		}
		if props.hasValues() {
			node.Props = props
		}
		node.Children = append(node.Children, left)
		node.Children = append(node.Children, b.planForSource(join.Right, nil))
		left = node
	}
	return left
}

func (b *physicalPlanBuilder) planForSource(source *validator.TableSource, choice *indexChoice) *PhysicalPlanNode {
	props := &PhysicalPlanProps{}
	tableName := source.Table.Name
	props.Table = &tableName
	node := &PhysicalPlanNode{Node: "SeqScan"}
	if choice != nil && choice.source == source {
		idxName := choice.info.def.Name
		props.Index = &idxName
		node.Node = "IndexScan"
	}
	if props.hasValues() {
		node.Props = props
	}
	return node
}

func (b *physicalPlanBuilder) wrapAggregation(child *PhysicalPlanNode) *PhysicalPlanNode {
	props := &PhysicalPlanProps{}
	if len(b.validated.Groupings) > 0 {
		groups := make([]string, len(b.validated.Groupings))
		for i, grouping := range b.validated.Groupings {
			groups[i] = grouping.Text
		}
		props.GroupKeys = groups
	}
	if aggs := b.buildAggregates(); len(aggs) > 0 {
		props.Aggs = aggs
	}
	node := &PhysicalPlanNode{Node: "HashAgg"}
	if props.hasValues() {
		node.Props = props
	}
	node.Children = append(node.Children, child)
	return node
}

func (b *physicalPlanBuilder) buildAggregates() []PhysicalPlanAggregate {
	if len(b.validated.Aggregates) == 0 {
		return nil
	}
	result := make([]PhysicalPlanAggregate, 0, len(b.validated.Aggregates))
	for _, def := range b.validated.Aggregates {
		fn, expr := parseAggregateCall(def.Name)
		agg := PhysicalPlanAggregate{Fn: fn, Expr: expr}
		if alias := b.aggAliases[def.Name]; alias != "" {
			agg.Alias = alias
		}
		result = append(result, agg)
	}
	return result
}

func (b *physicalPlanBuilder) wrapSort(child *PhysicalPlanNode) *PhysicalPlanNode {
	props := &PhysicalPlanProps{}
	orders := make([]PhysicalPlanOrder, len(b.validated.OrderBy))
	for i, term := range b.validated.OrderBy {
		dir := "ASC"
		if term.Desc {
			dir = "DESC"
		}
		orders[i] = PhysicalPlanOrder{Expr: term.Text, Dir: dir}
	}
	props.OrderBy = orders
	node := &PhysicalPlanNode{Node: "Sort", Props: props}
	node.Children = append(node.Children, child)
	return node
}

func joinAlgorithm(join *validator.JoinClause) string {
	if len(join.EquiConditions) > 0 {
		return "HashJoin"
	}
	return "NestedLoopJoin"
}

func newFilterNode(predicate string, child *PhysicalPlanNode) *PhysicalPlanNode {
	node := &PhysicalPlanNode{Node: "Filter"}
	if predicate != "" {
		props := &PhysicalPlanProps{}
		pred := predicate
		props.Predicate = &pred
		node.Props = props
	}
	node.Children = append(node.Children, child)
	return node
}

func wrapLimitNode(limit *parser.LimitClause, child *PhysicalPlanNode) *PhysicalPlanNode {
	if limit == nil {
		return child
	}
	props := &PhysicalPlanProps{}
	l := limit.Limit
	props.Limit = &l
	o := limit.Offset
	props.Offset = &o
	node := &PhysicalPlanNode{Node: "Limit", Props: props}
	node.Children = append(node.Children, child)
	return node
}

func gatherAggregateAliases(stmt *parser.SelectStmt) map[string]string {
	if stmt == nil {
		return nil
	}
	aliases := make(map[string]string)
	for _, item := range stmt.Items {
		exprItem, ok := item.(*parser.SelectExprItem)
		if !ok {
			continue
		}
		call, ok := exprItem.Expr.(*parser.FunctionCallExpr)
		if !ok {
			continue
		}
		if !isAggregateName(call.Name) {
			continue
		}
		if exprItem.Alias == "" {
			continue
		}
		key := parser.FormatExpression(exprItem.Expr)
		aliases[key] = exprItem.Alias
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

func isAggregateName(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func parseAggregateCall(call string) (string, string) {
	open := strings.Index(call, "(")
	close := strings.LastIndex(call, ")")
	if open > 0 && close > open {
		fn := strings.ToUpper(strings.TrimSpace(call[:open]))
		expr := strings.TrimSpace(call[open+1 : close])
		return fn, expr
	}
	trimmed := strings.TrimSpace(call)
	return strings.ToUpper(trimmed), ""
}

func formatJoinType(joinType validator.JoinType) string {
	switch joinType {
	case validator.JoinTypeLeft:
		return "Left"
	default:
		return "Inner"
	}
}

func (p *PhysicalPlanProps) hasValues() bool {
	if p == nil {
		return false
	}
	return p.Table != nil || p.Index != nil || p.Predicate != nil || len(p.OrderBy) > 0 || p.Limit != nil || p.Offset != nil ||
		len(p.GroupKeys) > 0 || len(p.Aggs) > 0 || p.JoinType != nil || p.Condition != nil || p.UsingIndexOrder != nil
}
