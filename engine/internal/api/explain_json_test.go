package api_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/example/granite-db/engine/internal/api"
)

type explainPayload struct {
	Version  int           `json:"version"`
	Physical *planJSONNode `json:"physical"`
	Text     string        `json:"text"`
}

type planJSONNode struct {
	Node     string          `json:"node"`
	Props    planJSONProps   `json:"props"`
	Children []*planJSONNode `json:"children"`
}

type planJSONProps struct {
	Table           *string     `json:"table"`
	Index           *string     `json:"index"`
	Predicate       *string     `json:"predicate"`
	OrderBy         []orderJSON `json:"orderBy"`
	Limit           *int        `json:"limit"`
	Offset          *int        `json:"offset"`
	GroupKeys       []string    `json:"groupKeys"`
	Aggs            []aggJSON   `json:"aggs"`
	JoinType        *string     `json:"joinType"`
	Condition       *string     `json:"condition"`
	UsingIndexOrder *bool       `json:"usingIndexOrder"`
}

type orderJSON struct {
	Expr string `json:"expr"`
	Dir  string `json:"dir"`
}

type aggJSON struct {
	Fn    string `json:"fn"`
	Expr  string `json:"expr"`
	Alias string `json:"alias"`
}

func TestExplainJSONSingleTablePipeline(t *testing.T) {
	db := prepareExplainDatabase(t)
	data := mustExplainJSON(t, db, "SELECT * FROM orders WHERE total > 50 ORDER BY total LIMIT 1;")
	payload := decodeExplainPayload(t, data)
	if payload.Version != 1 {
		t.Fatalf("expected version 1, got %d", payload.Version)
	}
	if payload.Physical == nil {
		t.Fatalf("expected physical plan present")
	}
	if payload.Text == "" {
		t.Fatalf("expected text plan to be populated")
	}
	project := payload.Physical
	if project.Node != "Project" {
		t.Fatalf("expected Project root, got %s", project.Node)
	}
	limit := singleChild(t, project)
	if limit.Node != "Limit" {
		t.Fatalf("expected Limit node, got %s", limit.Node)
	}
	if limit.Props.Limit == nil || *limit.Props.Limit != 1 {
		t.Fatalf("expected limit=1, got %+v", limit.Props.Limit)
	}
	if limit.Props.Offset == nil || *limit.Props.Offset != 0 {
		t.Fatalf("expected offset=0, got %+v", limit.Props.Offset)
	}
	sort := singleChild(t, limit)
	if sort.Node != "Sort" {
		t.Fatalf("expected Sort node, got %s", sort.Node)
	}
	if len(sort.Props.OrderBy) != 1 {
		t.Fatalf("expected single ORDER BY term, got %d", len(sort.Props.OrderBy))
	}
	term := sort.Props.OrderBy[0]
	if term.Expr != "total" || term.Dir != "ASC" {
		t.Fatalf("unexpected ORDER BY term: %+v", term)
	}
	filter := singleChild(t, sort)
	if filter.Node != "Filter" {
		t.Fatalf("expected Filter node, got %s", filter.Node)
	}
	if filter.Props.Predicate == nil || *filter.Props.Predicate != "total > 50" {
		t.Fatalf("unexpected predicate: %+v", filter.Props.Predicate)
	}
	scan := singleChild(t, filter)
	if scan.Node != "SeqScan" {
		t.Fatalf("expected SeqScan leaf, got %s", scan.Node)
	}
	if scan.Props.Table == nil || *scan.Props.Table != "orders" {
		t.Fatalf("expected table orders, got %+v", scan.Props.Table)
	}
	if scan.Props.Index != nil {
		t.Fatalf("did not expect index on seq scan: %+v", scan.Props.Index)
	}
}

func TestExplainJSONIndexScan(t *testing.T) {
	db := prepareExplainDatabase(t)
	data := mustExplainJSON(t, db, "SELECT name FROM customers WHERE id = 1;")
	payload := decodeExplainPayload(t, data)
	project := payload.Physical
	filter := singleChild(t, project)
	if filter.Node != "Filter" {
		t.Fatalf("expected Filter node, got %s", filter.Node)
	}
	if filter.Props.Predicate == nil || *filter.Props.Predicate != "id = 1" {
		t.Fatalf("unexpected predicate: %+v", filter.Props.Predicate)
	}
	scan := singleChild(t, filter)
	if scan.Node != "IndexScan" {
		t.Fatalf("expected IndexScan, got %s", scan.Node)
	}
	if scan.Props.Table == nil || *scan.Props.Table != "customers" {
		t.Fatalf("expected table customers, got %+v", scan.Props.Table)
	}
	if scan.Props.Index == nil || *scan.Props.Index != "idx_customers_id" {
		t.Fatalf("expected idx_customers_id, got %+v", scan.Props.Index)
	}
}

func TestExplainJSONJoins(t *testing.T) {
	db := prepareExplainDatabase(t)
	inner := decodeExplainPayload(t, mustExplainJSON(t, db, "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id;"))
	assertHashJoin(t, inner.Physical, "Inner")
	left := decodeExplainPayload(t, mustExplainJSON(t, db, "SELECT c.name, o.total FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;"))
	assertHashJoin(t, left.Physical, "Left")
}

func TestExplainJSONAggregation(t *testing.T) {
	db := prepareExplainDatabase(t)
	data := mustExplainJSON(t, db, "SELECT customer_id, SUM(total) AS spend FROM orders GROUP BY customer_id;")
	payload := decodeExplainPayload(t, data)
	project := payload.Physical
	agg := singleChild(t, project)
	if agg.Node != "HashAgg" {
		t.Fatalf("expected HashAgg node, got %s", agg.Node)
	}
	if len(agg.Props.GroupKeys) != 1 || agg.Props.GroupKeys[0] != "customer_id" {
		t.Fatalf("unexpected group keys: %+v", agg.Props.GroupKeys)
	}
	if len(agg.Props.Aggs) != 1 {
		t.Fatalf("expected one aggregate, got %d", len(agg.Props.Aggs))
	}
	entry := agg.Props.Aggs[0]
	if entry.Fn != "SUM" || entry.Expr != "total" {
		t.Fatalf("unexpected aggregate entry: %+v", entry)
	}
	if entry.Alias != "spend" {
		t.Fatalf("expected alias spend, got %q", entry.Alias)
	}
}

func assertHashJoin(t *testing.T, root *planJSONNode, joinType string) {
	t.Helper()
	project := root
	if project.Node != "Project" {
		t.Fatalf("expected Project root, got %s", project.Node)
	}
	join := singleChild(t, project)
	if join.Node != "HashJoin" {
		t.Fatalf("expected HashJoin node, got %s", join.Node)
	}
	if join.Props.JoinType == nil || *join.Props.JoinType != joinType {
		t.Fatalf("expected join type %s, got %+v", joinType, join.Props.JoinType)
	}
	if join.Props.Condition == nil || *join.Props.Condition != "c.id = o.customer_id" {
		t.Fatalf("unexpected join condition: %+v", join.Props.Condition)
	}
	if len(join.Children) != 2 {
		t.Fatalf("expected two children, got %d", len(join.Children))
	}
	if join.Children[0].Props.Table == nil || *join.Children[0].Props.Table != "customers" {
		t.Fatalf("expected left table customers, got %+v", join.Children[0].Props.Table)
	}
	if join.Children[1].Props.Table == nil || *join.Children[1].Props.Table != "orders" {
		t.Fatalf("expected right table orders, got %+v", join.Children[1].Props.Table)
	}
}

func prepareExplainDatabase(t *testing.T) *api.Database {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "plans.gdb")
	if err := api.Create(path); err != nil {
		t.Fatalf("create: %v", err)
	}
	db, err := api.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	mustExec(t, db, "CREATE TABLE customers(id INT PRIMARY KEY, name VARCHAR(50))")
	mustExec(t, db, "CREATE TABLE orders(id INT PRIMARY KEY, customer_id INT, total DECIMAL(10,2))")
	mustExec(t, db, "CREATE INDEX idx_customers_id ON customers(id)")
	mustExec(t, db, "INSERT INTO customers VALUES (1,'Ada'),(2,'Grace')")
	mustExec(t, db, "INSERT INTO orders VALUES (100,1,42.50),(101,2,99.99)")
	return db
}

func mustExplainJSON(t *testing.T, db *api.Database, sql string) []byte {
	t.Helper()
	data, err := db.ExplainJSON(sql)
	if err != nil {
		t.Fatalf("ExplainJSON %q: %v", sql, err)
	}
	return data
}

func decodeExplainPayload(t *testing.T, data []byte) explainPayload {
	t.Helper()
	var payload explainPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal explain JSON: %v", err)
	}
	return payload
}

func singleChild(t *testing.T, node *planJSONNode) *planJSONNode {
	t.Helper()
	if node == nil {
		t.Fatalf("node is nil")
	}
	if len(node.Children) != 1 {
		t.Fatalf("expected single child for %s, got %d", node.Node, len(node.Children))
	}
	return node.Children[0]
}
