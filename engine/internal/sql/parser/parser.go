package parser

import (
	"fmt"
	"strings"

	"github.com/example/granite-db/engine/internal/sql/lexer"
)

// Parse parses a single SQL statement into an AST.
func Parse(input string) (Statement, error) {
	p := &Parser{lex: lexer.New(input)}
	p.nextToken()
	p.nextToken()
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Allow optional trailing semicolon
	if p.curToken.Type == lexer.Semicolon {
		p.nextToken()
	}
	if p.curToken.Type != lexer.EOF {
		return nil, fmt.Errorf("parser: unexpected token %s", p.curToken.Literal)
	}
	return stmt, nil
}

// Parser implements a tiny hand-rolled recursive descent parser.
type Parser struct {
	lex       *lexer.Lexer
	curToken  lexer.Token
	peekToken lexer.Token
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lex.Next()
}

func (p *Parser) parseStatement() (Statement, error) {
	switch strings.ToUpper(p.curToken.Literal) {
	case "BEGIN":
		return p.parseBegin()
	case "START":
		return p.parseStart()
	case "COMMIT":
		return p.parseCommit()
	case "ROLLBACK":
		return p.parseRollback()
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("parser: unexpected token %s", p.curToken.Literal)
	}
}

func (p *Parser) parseBegin() (Statement, error) {
	if err := p.consumeKeyword("BEGIN"); err != nil {
		return nil, err
	}
	if strings.ToUpper(p.curToken.Literal) == "TRANSACTION" {
		p.nextToken()
	}
	return &BeginStmt{}, nil
}

func (p *Parser) parseStart() (Statement, error) {
	if err := p.consumeKeyword("START"); err != nil {
		return nil, err
	}
	if strings.ToUpper(p.curToken.Literal) != "TRANSACTION" {
		return nil, fmt.Errorf("parser: expected TRANSACTION after START")
	}
	p.nextToken()
	return &BeginStmt{}, nil
}

func (p *Parser) parseCommit() (Statement, error) {
	if err := p.consumeKeyword("COMMIT"); err != nil {
		return nil, err
	}
	if strings.ToUpper(p.curToken.Literal) == "TRANSACTION" {
		p.nextToken()
	}
	return &CommitStmt{}, nil
}

func (p *Parser) parseRollback() (Statement, error) {
	if err := p.consumeKeyword("ROLLBACK"); err != nil {
		return nil, err
	}
	if strings.ToUpper(p.curToken.Literal) == "TRANSACTION" {
		p.nextToken()
	}
	return &RollbackStmt{}, nil
}

func (p *Parser) expectKeyword(keyword string) error {
	if strings.ToUpper(p.curToken.Literal) != keyword {
		return fmt.Errorf("parser: expected %s but found %s", keyword, p.curToken.Literal)
	}
	return nil
}

func (p *Parser) consumeKeyword(keyword string) error {
	if err := p.expectKeyword(keyword); err != nil {
		return err
	}
	p.nextToken()
	return nil
}

func (p *Parser) parseCreate() (Statement, error) {
	if err := p.consumeKeyword("CREATE"); err != nil {
		return nil, err
	}
	unique := false
	if strings.ToUpper(p.curToken.Literal) == "UNIQUE" {
		unique = true
		p.nextToken()
	}
	switch strings.ToUpper(p.curToken.Literal) {
	case "TABLE":
		if unique {
			return nil, fmt.Errorf("parser: UNIQUE not valid before CREATE TABLE")
		}
		return p.parseCreateTable()
	case "INDEX":
		return p.parseCreateIndex(unique)
	default:
		if unique {
			return nil, fmt.Errorf("parser: expected INDEX after UNIQUE in CREATE")
		}
		return nil, fmt.Errorf("parser: expected TABLE or INDEX after CREATE, found %s", p.curToken.Literal)
	}
}

func (p *Parser) parseCreateTable() (Statement, error) {
	if err := p.consumeKeyword("TABLE"); err != nil {
		return nil, err
	}
	name := p.curToken.Literal
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name but found %s", p.curToken.Literal)
	}
	p.nextToken()
	if p.curToken.Type != lexer.LParen {
		return nil, fmt.Errorf("parser: expected ( after table name")
	}
	p.nextToken()

	cols := []ColumnDef{}
	foreignKeys := []ForeignKeyDef{}
	var primaryKey string
	for {
		upper := strings.ToUpper(p.curToken.Literal)
		switch upper {
		case "PRIMARY":
			if primaryKey != "" {
				return nil, fmt.Errorf("parser: primary key already defined")
			}
			pk, err := p.parsePrimaryKeyClause()
			if err != nil {
				return nil, err
			}
			primaryKey = pk
		case "CONSTRAINT":
			p.nextToken()
			if p.curToken.Type != lexer.Ident {
				return nil, fmt.Errorf("parser: expected constraint name")
			}
			constraintName := p.curToken.Literal
			p.nextToken()
			next := strings.ToUpper(p.curToken.Literal)
			switch next {
			case "PRIMARY":
				if primaryKey != "" {
					return nil, fmt.Errorf("parser: primary key already defined")
				}
				pk, err := p.parsePrimaryKeyClause()
				if err != nil {
					return nil, err
				}
				primaryKey = pk
			case "FOREIGN":
				fk, err := p.parseTableForeignKey(constraintName)
				if err != nil {
					return nil, err
				}
				foreignKeys = append(foreignKeys, fk)
			default:
				return nil, fmt.Errorf("parser: unsupported constraint type %s", p.curToken.Literal)
			}
		case "FOREIGN":
			fk, err := p.parseTableForeignKey("")
			if err != nil {
				return nil, err
			}
			foreignKeys = append(foreignKeys, fk)
		default:
			col, inline, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			if col.PrimaryKey {
				if primaryKey != "" {
					return nil, fmt.Errorf("parser: primary key already defined")
				}
				primaryKey = col.Name
			}
			cols = append(cols, col)
			if len(inline) > 0 {
				foreignKeys = append(foreignKeys, inline...)
			}
		}

		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			continue
		}
		break
	}

	if p.curToken.Type != lexer.RParen {
		return nil, fmt.Errorf("parser: expected ) to close column list")
	}
	p.nextToken()

	return &CreateTableStmt{Name: name, Columns: cols, PrimaryKey: primaryKey, ForeignKeys: foreignKeys}, nil
}

func (p *Parser) parseCreateIndex(unique bool) (Statement, error) {
	if err := p.consumeKeyword("INDEX"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected index name after CREATE INDEX")
	}
	name := p.curToken.Literal
	p.nextToken()
	if err := p.consumeKeyword("ON"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name after CREATE INDEX")
	}
	table := p.curToken.Literal
	p.nextToken()
	if p.curToken.Type != lexer.LParen {
		return nil, fmt.Errorf("parser: expected column list for CREATE INDEX")
	}
	p.nextToken()
	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("parser: CREATE INDEX requires at least one column")
	}
	return &CreateIndexStmt{Name: name, Table: table, Columns: columns, Unique: unique}, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, []ForeignKeyDef, error) {
	name := p.curToken.Literal
	if p.curToken.Type != lexer.Ident {
		return ColumnDef{}, nil, fmt.Errorf("parser: expected column name but found %s", p.curToken.Literal)
	}
	p.nextToken()

	colType, length, precision, scale, err := p.parseType()
	if err != nil {
		return ColumnDef{}, nil, err
	}
	notNull := false
	primaryKey := false
	pendingConstraint := ""
	inlineFKs := []ForeignKeyDef{}
	for {
		switch strings.ToUpper(p.curToken.Literal) {
		case "CONSTRAINT":
			if pendingConstraint != "" {
				return ColumnDef{}, nil, fmt.Errorf("parser: multiple constraint names on column %s", name)
			}
			p.nextToken()
			if p.curToken.Type != lexer.Ident {
				return ColumnDef{}, nil, fmt.Errorf("parser: expected constraint name for column %s", name)
			}
			pendingConstraint = p.curToken.Literal
			p.nextToken()
		case "NOT":
			if notNull {
				return ColumnDef{}, nil, fmt.Errorf("parser: NOT NULL specified multiple times")
			}
			p.nextToken()
			if strings.ToUpper(p.curToken.Literal) != "NULL" {
				return ColumnDef{}, nil, fmt.Errorf("parser: expected NULL after NOT")
			}
			p.nextToken()
			notNull = true
		case "PRIMARY":
			if primaryKey {
				return ColumnDef{}, nil, fmt.Errorf("parser: PRIMARY KEY specified multiple times")
			}
			p.nextToken()
			if strings.ToUpper(p.curToken.Literal) != "KEY" {
				return ColumnDef{}, nil, fmt.Errorf("parser: expected KEY after PRIMARY")
			}
			p.nextToken()
			primaryKey = true
			notNull = true
			pendingConstraint = ""
		case "REFERENCES":
			fk, err := p.parseInlineForeignKey(name, pendingConstraint)
			if err != nil {
				return ColumnDef{}, nil, err
			}
			inlineFKs = append(inlineFKs, fk)
			pendingConstraint = ""
		default:
			if pendingConstraint != "" {
				return ColumnDef{}, nil, fmt.Errorf("parser: constraint name %s must be followed by a constraint definition", pendingConstraint)
			}
			return ColumnDef{Name: name, Type: colType, Length: length, Precision: precision, Scale: scale, NotNull: notNull, PrimaryKey: primaryKey}, inlineFKs, nil
		}
	}
}

func (p *Parser) parsePrimaryKeyClause() (string, error) {
	if err := p.consumeKeyword("PRIMARY"); err != nil {
		return "", err
	}
	if err := p.consumeKeyword("KEY"); err != nil {
		return "", err
	}
	if p.curToken.Type != lexer.LParen {
		return "", fmt.Errorf("parser: expected ( after PRIMARY KEY")
	}
	p.nextToken()
	if p.curToken.Type != lexer.Ident {
		return "", fmt.Errorf("parser: expected column name in PRIMARY KEY")
	}
	column := p.curToken.Literal
	p.nextToken()
	if p.curToken.Type != lexer.RParen {
		return "", fmt.Errorf("parser: expected ) after PRIMARY KEY column")
	}
	p.nextToken()
	return column, nil
}

func (p *Parser) parseTableForeignKey(name string) (ForeignKeyDef, error) {
	if err := p.consumeKeyword("FOREIGN"); err != nil {
		return ForeignKeyDef{}, err
	}
	if err := p.consumeKeyword("KEY"); err != nil {
		return ForeignKeyDef{}, err
	}
	if p.curToken.Type != lexer.LParen {
		return ForeignKeyDef{}, fmt.Errorf("parser: expected ( after FOREIGN KEY")
	}
	p.nextToken()
	childCols, err := p.parseIdentifierList()
	if err != nil {
		return ForeignKeyDef{}, err
	}
	if len(childCols) == 0 {
		return ForeignKeyDef{}, fmt.Errorf("parser: FOREIGN KEY requires at least one column")
	}
	return p.parseForeignKeyReference(name, childCols)
}

func (p *Parser) parseInlineForeignKey(columnName, constraintName string) (ForeignKeyDef, error) {
	return p.parseForeignKeyReference(constraintName, []string{columnName})
}

func (p *Parser) parseForeignKeyReference(name string, childCols []string) (ForeignKeyDef, error) {
	if err := p.consumeKeyword("REFERENCES"); err != nil {
		return ForeignKeyDef{}, err
	}
	if p.curToken.Type != lexer.Ident {
		return ForeignKeyDef{}, fmt.Errorf("parser: expected referenced table name")
	}
	parentTable := p.curToken.Literal
	p.nextToken()
	if p.curToken.Type != lexer.LParen {
		return ForeignKeyDef{}, fmt.Errorf("parser: expected ( after referenced table name")
	}
	p.nextToken()
	parentCols, err := p.parseIdentifierList()
	if err != nil {
		return ForeignKeyDef{}, err
	}
	if len(parentCols) == 0 {
		return ForeignKeyDef{}, fmt.Errorf("parser: referenced column list cannot be empty")
	}
	fk := ForeignKeyDef{
		Name:            name,
		Columns:         childCols,
		ReferencedTable: parentTable,
		ReferencedCols:  parentCols,
		OnDelete:        ForeignKeyActionRestrict,
		OnUpdate:        ForeignKeyActionRestrict,
		Deferrable:      false,
	}
	deleteSet := false
	updateSet := false
	for strings.ToUpper(p.curToken.Literal) == "ON" {
		p.nextToken()
		modifier := strings.ToUpper(p.curToken.Literal)
		if modifier != "DELETE" && modifier != "UPDATE" {
			return ForeignKeyDef{}, fmt.Errorf("parser: expected DELETE or UPDATE after ON")
		}
		p.nextToken()
		action, err := p.parseReferentialAction()
		if err != nil {
			return ForeignKeyDef{}, err
		}
		if modifier == "DELETE" {
			if deleteSet {
				return ForeignKeyDef{}, fmt.Errorf("parser: ON DELETE specified multiple times")
			}
			fk.OnDelete = action
			deleteSet = true
		} else {
			if updateSet {
				return ForeignKeyDef{}, fmt.Errorf("parser: ON UPDATE specified multiple times")
			}
			fk.OnUpdate = action
			updateSet = true
		}
	}
	return fk, nil
}

func (p *Parser) parseReferentialAction() (ForeignKeyAction, error) {
	upper := strings.ToUpper(p.curToken.Literal)
	switch upper {
	case "RESTRICT":
		p.nextToken()
		return ForeignKeyActionRestrict, nil
	case "NO":
		p.nextToken()
		if strings.ToUpper(p.curToken.Literal) != "ACTION" {
			return ForeignKeyActionRestrict, fmt.Errorf("parser: expected ACTION after NO in referential action")
		}
		p.nextToken()
		return ForeignKeyActionNoAction, nil
	case "CASCADE":
		return ForeignKeyActionRestrict, fmt.Errorf("referential action CASCADE is not supported (yet)")
	case "SET":
		p.nextToken()
		action := strings.ToUpper(p.curToken.Literal)
		switch action {
		case "NULL", "DEFAULT":
			return ForeignKeyActionRestrict, fmt.Errorf("referential action SET %s is not supported (yet)", action)
		default:
			return ForeignKeyActionRestrict, fmt.Errorf("parser: unsupported referential action SET %s", p.curToken.Literal)
		}
	default:
		return ForeignKeyActionRestrict, fmt.Errorf("parser: unsupported referential action %s", p.curToken.Literal)
	}
}

func (p *Parser) parseType() (DataType, int, int, int, error) {
	switch strings.ToUpper(p.curToken.Literal) {
	case "INT":
		p.nextToken()
		return DataTypeInt, 0, 0, 0, nil
	case "BIGINT":
		p.nextToken()
		return DataTypeBigInt, 0, 0, 0, nil
	case "DECIMAL", "NUMERIC":
		p.nextToken()
		if p.curToken.Type != lexer.LParen {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected ( after DECIMAL")
		}
		p.nextToken()
		if p.curToken.Type != lexer.Number {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected precision for DECIMAL")
		}
		precision := parseInt(p.curToken.Literal)
		p.nextToken()
		scale := 0
		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			if p.curToken.Type != lexer.Number {
				return 0, 0, 0, 0, fmt.Errorf("parser: expected scale for DECIMAL")
			}
			scale = parseInt(p.curToken.Literal)
			p.nextToken()
		}
		if p.curToken.Type != lexer.RParen {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected ) after DECIMAL definition")
		}
		p.nextToken()
		return DataTypeDecimal, 0, precision, scale, nil
	case "BOOLEAN":
		p.nextToken()
		return DataTypeBoolean, 0, 0, 0, nil
	case "DATE":
		p.nextToken()
		return DataTypeDate, 0, 0, 0, nil
	case "TIMESTAMP":
		p.nextToken()
		return DataTypeTimestamp, 0, 0, 0, nil
	case "VARCHAR":
		p.nextToken()
		if p.curToken.Type != lexer.LParen {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected ( after VARCHAR")
		}
		p.nextToken()
		if p.curToken.Type != lexer.Number {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected length for VARCHAR")
		}
		length := p.curToken.Literal
		p.nextToken()
		if p.curToken.Type != lexer.RParen {
			return 0, 0, 0, 0, fmt.Errorf("parser: expected ) after VARCHAR length")
		}
		p.nextToken()
		return DataTypeVarChar, parseInt(length), 0, 0, nil
	default:
		return 0, 0, 0, 0, fmt.Errorf("parser: unknown type %s", p.curToken.Literal)
	}
}

func parseInt(value string) int {
	var result int
	for _, ch := range value {
		result = result*10 + int(ch-'0')
	}
	return result
}

func (p *Parser) parseDrop() (Statement, error) {
	if err := p.consumeKeyword("DROP"); err != nil {
		return nil, err
	}
	switch strings.ToUpper(p.curToken.Literal) {
	case "TABLE":
		p.nextToken()
		if p.curToken.Type != lexer.Ident {
			return nil, fmt.Errorf("parser: expected table name after DROP TABLE")
		}
		name := p.curToken.Literal
		p.nextToken()
		return &DropTableStmt{Name: name}, nil
	case "INDEX":
		p.nextToken()
		if p.curToken.Type != lexer.Ident {
			return nil, fmt.Errorf("parser: expected index name after DROP INDEX")
		}
		name := p.curToken.Literal
		p.nextToken()
		return &DropIndexStmt{Name: name}, nil
	default:
		return nil, fmt.Errorf("parser: expected TABLE or INDEX after DROP, found %s", p.curToken.Literal)
	}
}

func (p *Parser) parseInsert() (Statement, error) {
	if err := p.consumeKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.consumeKeyword("INTO"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name after INSERT INTO")
	}
	table := p.curToken.Literal
	p.nextToken()
	var columns []string
	hasColumns := false
	if p.curToken.Type == lexer.LParen {
		hasColumns = true
		p.nextToken()
		parsed, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		columns = parsed
	}
	if err := p.consumeKeyword("VALUES"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.LParen {
		return nil, fmt.Errorf("parser: expected value list in INSERT")
	}
	rows := make([][]Literal, 0, 1)
	for {
		p.nextToken()
		values, err := p.parseLiteralList()
		if err != nil {
			return nil, err
		}
		if hasColumns && len(columns) != len(values) {
			return nil, fmt.Errorf("parser: column count %d does not match value count %d", len(columns), len(values))
		}
		rows = append(rows, values)
		if p.curToken.Type != lexer.Comma {
			break
		}
		p.nextToken()
		if p.curToken.Type != lexer.LParen {
			return nil, fmt.Errorf("parser: expected ( to start next VALUES tuple")
		}
	}
	return &InsertStmt{Table: table, Columns: columns, Rows: rows}, nil
}

func (p *Parser) parseUpdate() (Statement, error) {
	if err := p.consumeKeyword("UPDATE"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name after UPDATE")
	}
	table := p.curToken.Literal
	p.nextToken()
	if err := p.consumeKeyword("SET"); err != nil {
		return nil, err
	}
	assignments := []UpdateAssignment{}
	for {
		if p.curToken.Type != lexer.Ident {
			return nil, fmt.Errorf("parser: expected column name in SET clause")
		}
		column := p.curToken.Literal
		p.nextToken()
		if p.curToken.Type != lexer.Equal {
			return nil, fmt.Errorf("parser: expected = in SET clause")
		}
		p.nextToken()
		expr, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, UpdateAssignment{Column: column, Expr: expr})
		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			continue
		}
		break
	}
	if len(assignments) == 0 {
		return nil, fmt.Errorf("parser: UPDATE requires at least one assignment")
	}
	var where Expression
	if strings.ToUpper(p.curToken.Literal) == "WHERE" {
		p.nextToken()
		var err error
		where, err = p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
	}
	return &UpdateStmt{Table: table, Assignments: assignments, Where: where}, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	if err := p.consumeKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.consumeKeyword("FROM"); err != nil {
		return nil, err
	}
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name after DELETE FROM")
	}
	table := p.curToken.Literal
	p.nextToken()
	var where Expression
	if strings.ToUpper(p.curToken.Literal) == "WHERE" {
		p.nextToken()
		var err error
		where, err = p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
	}
	return &DeleteStmt{Table: table, Where: where}, nil
}

func (p *Parser) parseSelect() (Statement, error) {
	if err := p.consumeKeyword("SELECT"); err != nil {
		return nil, err
	}
	items, err := p.parseSelectItems()
	if err != nil {
		return nil, err
	}
	stmt := &SelectStmt{Items: items}
	if strings.ToUpper(p.curToken.Literal) == "FROM" {
		p.nextToken()
		from, err := p.parseTableReference()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	if strings.ToUpper(p.curToken.Literal) == "WHERE" {
		p.nextToken()
		expr, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if strings.ToUpper(p.curToken.Literal) == "GROUP" {
		p.nextToken()
		if err := p.consumeKeyword("BY"); err != nil {
			return nil, err
		}
		groupExprs := []Expression{}
		for {
			expr, err := p.parseExpression(lowestPrecedence)
			if err != nil {
				return nil, err
			}
			groupExprs = append(groupExprs, expr)
			if p.curToken.Type != lexer.Comma {
				break
			}
			p.nextToken()
		}
		stmt.GroupBy = groupExprs
	}

	if strings.ToUpper(p.curToken.Literal) == "HAVING" {
		p.nextToken()
		having, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	if strings.ToUpper(p.curToken.Literal) == "ORDER" {
		p.nextToken()
		if err := p.consumeKeyword("BY"); err != nil {
			return nil, err
		}
		orders := []*OrderByExpr{}
		for {
			expr, err := p.parseExpression(lowestPrecedence)
			if err != nil {
				return nil, err
			}
			desc := false
			switch strings.ToUpper(p.curToken.Literal) {
			case "ASC":
				p.nextToken()
			case "DESC":
				desc = true
				p.nextToken()
			}
			orders = append(orders, &OrderByExpr{Expr: expr, Desc: desc})
			if p.curToken.Type != lexer.Comma {
				break
			}
			p.nextToken()
		}
		stmt.OrderBy = orders
	}

	if strings.ToUpper(p.curToken.Literal) == "LIMIT" {
		p.nextToken()
		if p.curToken.Type != lexer.Number {
			return nil, fmt.Errorf("parser: expected LIMIT value")
		}
		limit := parseInt(p.curToken.Literal)
		p.nextToken()
		offset := 0
		if strings.ToUpper(p.curToken.Literal) == "OFFSET" {
			p.nextToken()
			if p.curToken.Type != lexer.Number {
				return nil, fmt.Errorf("parser: expected OFFSET value")
			}
			offset = parseInt(p.curToken.Literal)
			p.nextToken()
		}
		stmt.Limit = &LimitClause{Limit: limit, Offset: offset}
	}

	return stmt, nil
}

func (p *Parser) parseTableReference() (TableExpr, error) {
	left, err := p.parseTableFactor()
	if err != nil {
		return nil, err
	}
	for {
		joinType, ok, err := p.parseJoinType()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		right, err := p.parseTableFactor()
		if err != nil {
			return nil, err
		}
		if strings.ToUpper(p.curToken.Literal) == "USING" {
			return nil, fmt.Errorf("parser: USING is not supported for joins")
		}
		if err := p.consumeKeyword("ON"); err != nil {
			return nil, err
		}
		condition, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		left = &JoinExpr{Left: left, Right: right, Type: joinType, Condition: condition}
	}
	return left, nil
}

func (p *Parser) parseTableFactor() (TableExpr, error) {
	if p.curToken.Type != lexer.Ident {
		return nil, fmt.Errorf("parser: expected table name in FROM clause")
	}
	name := p.curToken.Literal
	p.nextToken()
	alias := ""
	switch {
	case strings.ToUpper(p.curToken.Literal) == "AS":
		p.nextToken()
		if p.curToken.Type != lexer.Ident {
			return nil, fmt.Errorf("parser: expected alias after AS")
		}
		alias = p.curToken.Literal
		p.nextToken()
	case p.curToken.Type == lexer.Ident && !isAliasTerminator(strings.ToUpper(p.curToken.Literal)):
		alias = p.curToken.Literal
		p.nextToken()
	}
	return &TableName{Name: name, Alias: alias}, nil
}

func (p *Parser) parseJoinType() (JoinType, bool, error) {
	switch strings.ToUpper(p.curToken.Literal) {
	case "JOIN":
		p.nextToken()
		return JoinTypeInner, true, nil
	case "INNER":
		p.nextToken()
		if err := p.consumeKeyword("JOIN"); err != nil {
			return 0, false, err
		}
		return JoinTypeInner, true, nil
	case "LEFT":
		p.nextToken()
		if strings.ToUpper(p.curToken.Literal) == "OUTER" {
			p.nextToken()
		}
		if err := p.consumeKeyword("JOIN"); err != nil {
			return 0, false, err
		}
		return JoinTypeLeft, true, nil
	default:
		return 0, false, nil
	}
}

func (p *Parser) parseSelectItems() ([]SelectItem, error) {
	if p.curToken.Type == lexer.Star {
		p.nextToken()
		if p.curToken.Type == lexer.Comma {
			return nil, fmt.Errorf("parser: SELECT * cannot be combined with expressions (yet)")
		}
		return []SelectItem{&SelectStarItem{}}, nil
	}

	items := []SelectItem{}
	for {
		expr, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		alias := ""
		switch {
		case strings.ToUpper(p.curToken.Literal) == "AS":
			p.nextToken()
			if p.curToken.Type != lexer.Ident {
				return nil, fmt.Errorf("parser: expected alias after AS")
			}
			alias = p.curToken.Literal
			p.nextToken()
		case p.curToken.Type == lexer.Ident && !isAliasTerminator(strings.ToUpper(p.curToken.Literal)):
			alias = p.curToken.Literal
			p.nextToken()
		}
		items = append(items, &SelectExprItem{Expr: expr, Alias: alias})
		if p.curToken.Type != lexer.Comma {
			break
		}
		p.nextToken()
	}
	return items, nil
}

func (p *Parser) parseIdentifierList() ([]string, error) {
	values := []string{}
	for {
		if p.curToken.Type != lexer.Ident {
			return nil, fmt.Errorf("parser: expected identifier")
		}
		values = append(values, p.curToken.Literal)
		p.nextToken()
		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			continue
		}
		if p.curToken.Type == lexer.RParen {
			p.nextToken()
			break
		}
		return nil, fmt.Errorf("parser: unexpected token in identifier list: %s", p.curToken.Literal)
	}
	return values, nil
}

func (p *Parser) parseLiteralList() ([]Literal, error) {
	values := []Literal{}
	for {
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		values = append(values, lit)
		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			continue
		}
		if p.curToken.Type == lexer.RParen {
			p.nextToken()
			break
		}
		return nil, fmt.Errorf("parser: unexpected token in value list: %s", p.curToken.Literal)
	}
	return values, nil
}

func (p *Parser) parseLiteral() (Literal, error) {
	switch p.curToken.Type {
	case lexer.String:
		lit := Literal{Kind: LiteralString, Value: p.curToken.Literal}
		p.nextToken()
		return lit, nil
	case lexer.Number:
		kind := LiteralNumber
		if strings.Contains(p.curToken.Literal, ".") {
			kind = LiteralDecimal
		}
		lit := Literal{Kind: kind, Value: p.curToken.Literal}
		p.nextToken()
		return lit, nil
	case lexer.Ident:
		upper := strings.ToUpper(p.curToken.Literal)
		switch upper {
		case "TRUE", "FALSE":
			lit := Literal{Kind: LiteralBoolean, Value: upper}
			p.nextToken()
			return lit, nil
		case "NULL":
			lit := Literal{Kind: LiteralNull, Value: upper}
			p.nextToken()
			return lit, nil
		case "DATE":
			return p.parseTypedLiteral(LiteralDate)
		case "TIMESTAMP":
			return p.parseTypedLiteral(LiteralTimestamp)
		}
	}
	return Literal{}, fmt.Errorf("parser: unsupported literal %s", p.curToken.Literal)
}

func (p *Parser) parseTypedLiteral(kind LiteralKind) (Literal, error) {
	p.nextToken()
	if p.curToken.Type != lexer.String {
		return Literal{}, fmt.Errorf("parser: expected string literal after type keyword")
	}
	lit := Literal{Kind: kind, Value: p.curToken.Literal}
	p.nextToken()
	return lit, nil
}

func (p *Parser) parseExpression(precedence int) (Expression, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}
	for {
		if strings.ToUpper(p.curToken.Literal) == "IS" {
			if precedence >= comparisonPrecedence {
				break
			}
			expr, err := p.parseIsNull(left)
			if err != nil {
				return nil, err
			}
			left = expr
			continue
		}
		curPrec := p.curPrecedence()
		if precedence >= curPrec {
			break
		}
		switch {
		case isArithmeticToken(p.curToken.Type):
			tok := p.curToken
			op, ok := binaryOpForToken(tok)
			if !ok {
				return nil, fmt.Errorf("parser: unexpected operator %s", tok.Literal)
			}
			prec := p.curPrecedence()
			p.nextToken()
			right, err := p.parseExpression(prec)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: op}
		case isComparisonToken(p.curToken.Type):
			tok := p.curToken
			op, ok := binaryOpForToken(tok)
			if !ok {
				return nil, fmt.Errorf("parser: unexpected comparison operator %s", tok.Literal)
			}
			prec := p.curPrecedence()
			p.nextToken()
			right, err := p.parseExpression(prec)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: op}
		case strings.ToUpper(p.curToken.Literal) == "AND":
			p.nextToken()
			right, err := p.parseExpression(andPrecedence)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: BinaryAnd}
		case strings.ToUpper(p.curToken.Literal) == "OR":
			p.nextToken()
			right, err := p.parseExpression(orPrecedence)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: BinaryOr}
		default:
			return left, nil
		}
	}
	return left, nil
}

func (p *Parser) parsePrefix() (Expression, error) {
	switch p.curToken.Type {
	case lexer.Plus:
		p.nextToken()
		expr, err := p.parseExpression(prefixPrecedence)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: UnaryPlus, Expr: expr}, nil
	case lexer.Minus:
		p.nextToken()
		expr, err := p.parseExpression(prefixPrecedence)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: UnaryMinus, Expr: expr}, nil
	case lexer.Ident:
		name := p.curToken.Literal
		upper := strings.ToUpper(name)
		switch upper {
		case "TRUE", "FALSE":
			lit := Literal{Kind: LiteralBoolean, Value: upper}
			p.nextToken()
			return &LiteralExpr{Literal: lit}, nil
		case "NULL":
			lit := Literal{Kind: LiteralNull, Value: upper}
			p.nextToken()
			return &LiteralExpr{Literal: lit}, nil
		case "NOT":
			p.nextToken()
			expr, err := p.parseExpression(prefixPrecedence)
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: UnaryNot, Expr: expr}, nil
		case "DATE":
			lit, err := p.parseTypedLiteral(LiteralDate)
			if err != nil {
				return nil, err
			}
			return &LiteralExpr{Literal: lit}, nil
		case "TIMESTAMP":
			lit, err := p.parseTypedLiteral(LiteralTimestamp)
			if err != nil {
				return nil, err
			}
			return &LiteralExpr{Literal: lit}, nil
		}
		if p.peekToken.Type == lexer.LParen {
			return p.parseFunctionCall(upper)
		}
		table := ""
		if p.peekToken.Type == lexer.Dot {
			table = name
			p.nextToken()
			if p.curToken.Type != lexer.Dot {
				return nil, fmt.Errorf("parser: expected . in column reference")
			}
			p.nextToken()
			if p.curToken.Type != lexer.Ident {
				return nil, fmt.Errorf("parser: expected column name after .")
			}
			name = p.curToken.Literal
		}
		p.nextToken()
		return &ColumnRef{Table: table, Name: name}, nil
	case lexer.String:
		lit := Literal{Kind: LiteralString, Value: p.curToken.Literal}
		p.nextToken()
		return &LiteralExpr{Literal: lit}, nil
	case lexer.Number:
		kind := LiteralNumber
		if strings.Contains(p.curToken.Literal, ".") {
			kind = LiteralDecimal
		}
		lit := Literal{Kind: kind, Value: p.curToken.Literal}
		p.nextToken()
		return &LiteralExpr{Literal: lit}, nil
	case lexer.LParen:
		p.nextToken()
		expr, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		if p.curToken.Type != lexer.RParen {
			return nil, fmt.Errorf("parser: expected ) to close expression")
		}
		p.nextToken()
		return expr, nil
	default:
		return nil, fmt.Errorf("parser: unexpected token %s in expression", p.curToken.Literal)
	}
}

func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	p.nextToken()
	if p.curToken.Type != lexer.LParen {
		return nil, fmt.Errorf("parser: expected ( after function name %s", name)
	}
	p.nextToken()

	fn := &FunctionCallExpr{Name: name}
	if strings.ToUpper(p.curToken.Literal) == "DISTINCT" {
		fn.Distinct = true
		p.nextToken()
	}

	if p.curToken.Type == lexer.Star {
		fn.Args = append(fn.Args, &StarExpr{})
		p.nextToken()
		if p.curToken.Type != lexer.RParen {
			return nil, fmt.Errorf("parser: COUNT(*) style arguments cannot combine with others")
		}
		p.nextToken()
		return fn, nil
	}

	if p.curToken.Type == lexer.RParen {
		p.nextToken()
		return fn, nil
	}

	args := []Expression{}
	for {
		arg, err := p.parseExpression(lowestPrecedence)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if p.curToken.Type == lexer.Comma {
			p.nextToken()
			continue
		}
		if p.curToken.Type != lexer.RParen {
			return nil, fmt.Errorf("parser: expected ) to close function %s", name)
		}
		break
	}
	fn.Args = args
	p.nextToken()
	return fn, nil
}

func (p *Parser) parseIsNull(left Expression) (Expression, error) {
	if err := p.consumeKeyword("IS"); err != nil {
		return nil, err
	}
	negated := false
	if strings.ToUpper(p.curToken.Literal) == "NOT" {
		p.nextToken()
		negated = true
	}
	if strings.ToUpper(p.curToken.Literal) != "NULL" {
		return nil, fmt.Errorf("parser: expected NULL after IS")
	}
	p.nextToken()
	return &IsNullExpr{Expr: left, Negated: negated}, nil
}

func isComparisonToken(tt lexer.TokenType) bool {
	switch tt {
	case lexer.Equal, lexer.NotEqual, lexer.Less, lexer.LessEqual, lexer.Greater, lexer.GreaterEqual:
		return true
	default:
		return false
	}
}

func isArithmeticToken(tt lexer.TokenType) bool {
	switch tt {
	case lexer.Plus, lexer.Minus, lexer.Star, lexer.Slash, lexer.Percent:
		return true
	default:
		return false
	}
}

func binaryOpForToken(tok lexer.Token) (BinaryOp, bool) {
	switch tok.Type {
	case lexer.Plus:
		return BinaryAdd, true
	case lexer.Minus:
		return BinarySubtract, true
	case lexer.Star:
		return BinaryMultiply, true
	case lexer.Slash:
		return BinaryDivide, true
	case lexer.Percent:
		return BinaryModulo, true
	case lexer.Equal:
		return BinaryEqual, true
	case lexer.NotEqual:
		return BinaryNotEqual, true
	case lexer.Less:
		return BinaryLess, true
	case lexer.LessEqual:
		return BinaryLessEqual, true
	case lexer.Greater:
		return BinaryGreater, true
	case lexer.GreaterEqual:
		return BinaryGreaterEqual, true
	default:
		return "", false
	}
}

const (
	lowestPrecedence         = 0
	orPrecedence             = 1
	andPrecedence            = 2
	comparisonPrecedence     = 3
	additivePrecedence       = 4
	multiplicativePrecedence = 5
	prefixPrecedence         = 6
)

func (p *Parser) curPrecedence() int {
	switch {
	case p.curToken.Type == lexer.Plus || p.curToken.Type == lexer.Minus:
		return additivePrecedence
	case p.curToken.Type == lexer.Star || p.curToken.Type == lexer.Slash || p.curToken.Type == lexer.Percent:
		return multiplicativePrecedence
	case isComparisonToken(p.curToken.Type):
		return comparisonPrecedence
	case strings.ToUpper(p.curToken.Literal) == "AND":
		return andPrecedence
	case strings.ToUpper(p.curToken.Literal) == "OR":
		return orPrecedence
	case strings.ToUpper(p.curToken.Literal) == "IS":
		return comparisonPrecedence
	default:
		return lowestPrecedence
	}
}

func isAliasTerminator(lit string) bool {
	switch lit {
	case "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "BY", "LIMIT", "OFFSET", "ASC", "DESC", "AND", "OR", "JOIN", "INNER", "LEFT", "OUTER", "ON", "USING":
		return true
	default:
		return false
	}
}
