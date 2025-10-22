package exec

// Plan describes a tree of executor operations produced for a SQL statement.
type Plan struct {
	Root *PlanNode `json:"root"`
}

// PlanNode is an individual operator in the execution tree.
type PlanNode struct {
	Name     string                 `json:"name"`
	Detail   map[string]interface{} `json:"detail,omitempty"`
	Children []*PlanNode            `json:"children,omitempty"`
}

// newPlan creates a plan with the provided root node.
func newPlan(name string, detail map[string]interface{}) *Plan {
	return &Plan{Root: &PlanNode{Name: name, Detail: detail}}
}
