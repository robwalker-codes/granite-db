package main

import (
	"flag"
	"fmt"
	"os"
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
	fmt.Println("  granitectl exec -q <SQL> <dbfile>")
	fmt.Println("  granitectl dump <dbfile>")
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
	fs.Usage = func() {
		fmt.Println("Usage: granitectl exec -q <SQL> <dbfile>")
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

	result, err := db.Execute(*query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	renderResult(result)
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
		fmt.Println()
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

func renderResult(res *exec.Result) {
	if len(res.Columns) == 0 {
		fmt.Println(res.Message)
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
	printRow(res.Columns, widths)
	separator := make([]string, len(widths))
	for i, w := range widths {
		separator[i] = strings.Repeat("-", w)
	}
	printRow(separator, widths)
	for _, row := range res.Rows {
		printRow(row, widths)
	}
	fmt.Printf("(%d row(s))\n", len(res.Rows))
}

func printRow(values []string, widths []int) {
	cells := make([]string, len(values))
	for i, v := range values {
		cells[i] = fmt.Sprintf("%-*s", widths[i], v)
	}
	fmt.Println(strings.Join(cells, " | "))
}
