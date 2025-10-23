package exec

import (
	"fmt"
	"strings"
)

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

// Text renders the plan using the compact tree layout employed by the CLI.
func (p *Plan) Text() string {
	if p == nil || p.Root == nil {
		return ""
	}
	var builder strings.Builder
	writePlanNode(&builder, p.Root, 0)
	return strings.TrimRight(builder.String(), "\n")
}

func writePlanNode(builder *strings.Builder, node *PlanNode, depth int) {
	if node == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	builder.WriteString(indent)
	builder.WriteString("- ")
	builder.WriteString(node.Name)
	if len(node.Detail) > 0 {
		builder.WriteString(" ")
		builder.WriteString(fmt.Sprintf("%v", node.Detail))
	}
	builder.WriteString("\n")
	for _, child := range node.Children {
		writePlanNode(builder, child, depth+1)
	}
}

// newPlan creates a plan with the provided root node.
func newPlan(name string, detail map[string]interface{}) *Plan {
	return &Plan{Root: &PlanNode{Name: name, Detail: detail}}
}
