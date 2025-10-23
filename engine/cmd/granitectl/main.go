package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/example/granite-db/engine/internal/api"
	"github.com/example/granite-db/engine/internal/catalog"
	"github.com/example/granite-db/engine/internal/exec"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	cmd := os.Args[1]
	switch cmd {
	case "new":
		runNew(os.Args[2:])
	case "exec":
		runExec(os.Args[2:])
	case "dump":
		runDump(os.Args[2:])
	case "explain":
		runExplain(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("GraniteDB control utility")
	fmt.Println("Usage:")
	fmt.Println("  granitectl new <dbfile>")
	fmt.Println("  granitectl exec [-q <SQL> | -f <file.sql>] [--format table|csv] [--continue-on-error] <dbfile>")
	fmt.Println("  granitectl dump <dbfile>")
	fmt.Println("  granitectl explain -q <SQL> [--json] [--out <file>] <dbfile>")
}

func runNew(args []string) {
	fs := flag.NewFlagSet("new", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println("Usage: granitectl new <dbfile>")
	}
	fs.Parse(args)
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(1)
	}
	path := fs.Arg(0)
	if err := api.Create(path); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created database %s\n", path)
}

func runExec(args []string) {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	query := fs.String("q", "", "SQL query to execute")
	script := fs.String("f", "", "Path to SQL script file")
	format := fs.String("format", "table", "Output format: table or csv")
	continueOnError := fs.Bool("continue-on-error", false, "Continue script execution after errors")
	fs.Usage = func() {
		fmt.Println("Usage: granitectl exec [-q <SQL> | -f <file.sql>] [--format table|csv] [--continue-on-error] <dbfile>")
	}
	fs.Parse(args)
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(1)
	}
	if (*query == "" && *script == "") || (*query != "" && *script != "") {
		fmt.Fprintln(os.Stderr, "error: either -q or -f must be provided")
		os.Exit(1)
	}
	if err := validateFormat(*format); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	db, err := api.Open(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if *query != "" {
		statements := splitStatements(*query)
		if len(statements) == 0 {
			fmt.Fprintln(os.Stderr, "error: no statements to execute")
			os.Exit(1)
		}
		for _, stmt := range statements {
			result, err := db.Execute(stmt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
			if err := renderResult(os.Stdout, result, *format); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
		}
		return
	}

	if err := execScript(db, *script, *format, *continueOnError); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func runDump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println("Usage: granitectl dump <dbfile>")
	}
	fs.Parse(args)
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(1)
	}
	db, err := api.Open(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	tables, err := db.Tables()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if len(tables) == 0 {
		fmt.Println("No tables defined")
		return
	}
	for _, table := range tables {
		fmt.Printf("Table %s (%d row(s))\n", table.Name, table.RowCount)
		for _, col := range table.Columns {
			fmt.Printf("  - %s %s", col.Name, describeType(col))
			if col.NotNull {
				fmt.Print(" NOT NULL")
			}
			if col.PrimaryKey {
				fmt.Print(" PRIMARY KEY")
			}
			fmt.Println()
		}
		if len(table.Indexes) > 0 {
			fmt.Println("  Indexes:")
			indexes := make([]*catalog.Index, 0, len(table.Indexes))
			for _, idx := range table.Indexes {
				indexes = append(indexes, idx)
			}
			sort.Slice(indexes, func(i, j int) bool {
				return strings.ToLower(indexes[i].Name) < strings.ToLower(indexes[j].Name)
			})
			for _, idx := range indexes {
				fmt.Printf("    - %s (%s)", idx.Name, strings.Join(idx.Columns, ", "))
				if idx.IsUnique {
					fmt.Print(" UNIQUE")
				}
				fmt.Println()
			}
		}
		fmt.Println()
	}
}

func runExplain(args []string) {
	fs := flag.NewFlagSet("explain", flag.ExitOnError)
	query := fs.String("q", "", "SQL query to explain")
	jsonOnly := fs.Bool("json", false, "Output plan as JSON only")
	outPath := fs.String("out", "", "Write JSON plan to the specified file")
	fs.Usage = func() {
		fmt.Println("Usage: granitectl explain -q <SQL> [--json] [--out <file>] <dbfile>")
	}
	fs.Parse(args)
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(1)
	}
	if *query == "" {
		fmt.Fprintln(os.Stderr, "error: -q is required")
		os.Exit(1)
	}
	db, err := api.Open(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	var plan *exec.Plan
	if !*jsonOnly {
		plan, err = db.Explain(*query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}
	jsonData, err := db.ExplainJSON(*query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, jsonData, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}
	if *jsonOnly {
		if err := printJSON(jsonData); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	prettyPrintPlan(plan.Root, 0)
	if err := renderPlanJSON(jsonData); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func describeType(col catalog.Column) string {
	switch col.Type {
	case catalog.ColumnTypeInt:
		return "INT"
	case catalog.ColumnTypeBigInt:
		return "BIGINT"
	case catalog.ColumnTypeVarChar:
		return fmt.Sprintf("VARCHAR(%d)", col.Length)
	case catalog.ColumnTypeDecimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", col.Precision, col.Scale)
	case catalog.ColumnTypeBoolean:
		return "BOOLEAN"
	case catalog.ColumnTypeDate:
		return "DATE"
	case catalog.ColumnTypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

func renderResult(w io.Writer, res *exec.Result, format string) error {
	switch format {
	case "table":
		renderTable(w, res)
		return nil
	case "csv":
		return renderCSV(w, res)
	default:
		return fmt.Errorf("unsupported format %s", format)
	}
}

func renderTable(w io.Writer, res *exec.Result) {
	if len(res.Columns) == 0 {
		fmt.Fprintln(w, res.Message)
		return
	}
	widths := make([]int, len(res.Columns))
	for i, col := range res.Columns {
		widths[i] = len(col)
	}
	for _, row := range res.Rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	printRow(w, res.Columns, widths)
	separator := make([]string, len(widths))
	for i, w := range widths {
		separator[i] = strings.Repeat("-", w)
	}
	printRow(w, separator, widths)
	for _, row := range res.Rows {
		printRow(w, row, widths)
	}
	fmt.Fprintf(w, "(%d row(s))\n", len(res.Rows))
}

func renderCSV(w io.Writer, res *exec.Result) error {
	writer := csv.NewWriter(w)
	if len(res.Columns) > 0 {
		if err := writer.Write(res.Columns); err != nil {
			return err
		}
	}
	for _, row := range res.Rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	if len(res.Columns) == 0 {
		fmt.Fprintln(w, res.Message)
	}
	return nil
}

func printRow(w io.Writer, values []string, widths []int) {
	cells := make([]string, len(values))
	for i, v := range values {
		cells[i] = fmt.Sprintf("%-*s", widths[i], v)
	}
	fmt.Fprintln(w, strings.Join(cells, " | "))
}

func validateFormat(format string) error {
	switch format {
	case "table", "csv":
		return nil
	default:
		return fmt.Errorf("unknown format %s", format)
	}
}

func execScript(db *api.Database, path, format string, continueOnError bool) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	statements := splitStatements(string(content))
	var combinedErr error
	for _, stmt := range statements {
		res, err := db.Execute(stmt)
		if err != nil {
			if continueOnError {
				fmt.Fprintf(os.Stderr, "error executing %q: %v\n", stmt, err)
				combinedErr = err
				continue
			}
			return fmt.Errorf("statement %q failed: %w", stmt, err)
		}
		if err := renderResult(os.Stdout, res, format); err != nil {
			return err
		}
	}
	if combinedErr != nil {
		return fmt.Errorf("one or more statements failed; see above")
	}
	return nil
}

func splitStatements(script string) []string {
	parts := strings.Split(script, ";")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func prettyPrintPlan(node *exec.PlanNode, depth int) {
	if node == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	fmt.Printf("%s- %s", indent, node.Name)
	if len(node.Detail) > 0 {
		fmt.Printf(" %v", node.Detail)
	}
	fmt.Println()
	for _, child := range node.Children {
		prettyPrintPlan(child, depth+1)
	}
}

func renderPlanJSON(data []byte) error {
	pretty, err := indentJSON(data)
	if err != nil {
		return err
	}
	fmt.Println("\nPlan JSON:")
	fmt.Println(pretty)
	return nil
}

func printJSON(data []byte) error {
	pretty, err := indentJSON(data)
	if err != nil {
		return err
	}
	fmt.Println(pretty)
	return nil
}

func indentJSON(data []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		return "", err
	}
	return buf.String(), nil
}
