package lexer

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType identifies lexical tokens produced by the SQL lexer.
type TokenType int

const (
	EOF TokenType = iota
	Illegal
	Ident
	Number
	String
	Comma
	LParen
	RParen
	Semicolon
	Star
	Plus
	Minus
	Slash
	Percent
	Dot
	Equal
	NotEqual
	Less
	LessEqual
	Greater
	GreaterEqual
)

// Token represents a lexical item.
type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]TokenType{
	"ACTION":      Ident,
	"AND":         Ident,
	"ASC":         Ident,
	"BEGIN":       Ident,
	"BIGINT":      Ident,
	"BOOLEAN":     Ident,
	"BY":          Ident,
	"CASCADE":     Ident,
	"COALESCE":    Ident,
	"COMMIT":      Ident,
	"CONSTRAINT":  Ident,
	"CREATE":      Ident,
	"DATE":        Ident,
	"DECIMAL":     Ident,
	"DELETE":      Ident,
	"DESC":        Ident,
	"DROP":        Ident,
	"FALSE":       Ident,
	"FOREIGN":     Ident,
	"FROM":        Ident,
	"INNER":       Ident,
	"INSERT":      Ident,
	"INT":         Ident,
	"INTO":        Ident,
	"JOIN":        Ident,
	"KEY":         Ident,
	"LENGTH":      Ident,
	"LEFT":        Ident,
	"LIMIT":       Ident,
	"LOWER":       Ident,
	"NO":          Ident,
	"NOT":         Ident,
	"NULL":        Ident,
	"OFFSET":      Ident,
	"ON":          Ident,
	"OR":          Ident,
	"ORDER":       Ident,
	"OUTER":       Ident,
	"PRIMARY":     Ident,
	"REFERENCES":  Ident,
	"RESTRICT":    Ident,
	"ROLLBACK":    Ident,
	"SELECT":      Ident,
	"SET":         Ident,
	"START":       Ident,
	"TABLE":       Ident,
	"TIMESTAMP":   Ident,
	"TRANSACTION": Ident,
	"TRUE":        Ident,
	"UPDATE":      Ident,
	"UPPER":       Ident,
	"USING":       Ident,
	"VALUES":      Ident,
	"VARCHAR":     Ident,
	"WHERE":       Ident,
}

// Lexer performs tokenisation over the input SQL string.
type Lexer struct {
	input []rune
	pos   int
}

// New initialises a lexer for the provided SQL source.
func New(input string) *Lexer {
	return &Lexer{input: []rune(input)}
}

// Next returns the next token from the stream.
func (l *Lexer) Next() Token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Type: EOF}
	}

	ch := l.input[l.pos]
	switch ch {
	case ',':
		l.pos++
		return Token{Type: Comma, Literal: ","}
	case '(':
		l.pos++
		return Token{Type: LParen, Literal: "("}
	case ')':
		l.pos++
		return Token{Type: RParen, Literal: ")"}
	case ';':
		l.pos++
		return Token{Type: Semicolon, Literal: ";"}
	case '*':
		l.pos++
		return Token{Type: Star, Literal: "*"}
	case '+':
		l.pos++
		return Token{Type: Plus, Literal: "+"}
	case '-':
		l.pos++
		return Token{Type: Minus, Literal: "-"}
	case '/':
		l.pos++
		return Token{Type: Slash, Literal: "/"}
	case '%':
		l.pos++
		return Token{Type: Percent, Literal: "%"}
	case '.':
		l.pos++
		return Token{Type: Dot, Literal: "."}
	case '=':
		l.pos++
		return Token{Type: Equal, Literal: "="}
	case '<':
		l.pos++
		if l.pos < len(l.input) {
			switch l.input[l.pos] {
			case '=':
				l.pos++
				return Token{Type: LessEqual, Literal: "<="}
			case '>':
				l.pos++
				return Token{Type: NotEqual, Literal: "<>"}
			}
		}
		return Token{Type: Less, Literal: "<"}
	case '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return Token{Type: GreaterEqual, Literal: ">="}
		}
		return Token{Type: Greater, Literal: ">"}
	case '\'', '"':
		return l.scanString(ch)
	}

	if unicode.IsLetter(ch) || ch == '_' {
		return l.scanIdentifier()
	}
	if unicode.IsDigit(ch) {
		return l.scanNumber()
	}

	l.pos++
	return Token{Type: Illegal, Literal: string(ch)}
}

func (l *Lexer) scanIdentifier() Token {
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' {
			l.pos++
			continue
		}
		break
	}
	lit := string(l.input[start:l.pos])
	upper := strings.ToUpper(lit)
	if _, ok := keywords[upper]; ok {
		return Token{Type: Ident, Literal: upper}
	}
	return Token{Type: Ident, Literal: lit}
}

func (l *Lexer) scanNumber() Token {
	start := l.pos
	seenDot := false
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsDigit(ch) {
			l.pos++
			continue
		}
		if ch == '.' && !seenDot {
			seenDot = true
			l.pos++
			continue
		}
		break
	}
	return Token{Type: Number, Literal: string(l.input[start:l.pos])}
}

func (l *Lexer) scanString(quote rune) Token {
	l.pos++
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == quote {
			lit := string(l.input[start:l.pos])
			l.pos++
			return Token{Type: String, Literal: strings.ReplaceAll(lit, "''", "'")}
		}
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos += 2
			continue
		}
		l.pos++
	}
	return Token{Type: Illegal, Literal: "unterminated string literal"}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		if unicode.IsSpace(l.input[l.pos]) {
			l.pos++
			continue
		}
		break
	}
}

// Expect consumes the next token if it matches the provided literal.
func (l *Lexer) Expect(lit string) error {
	tok := l.Next()
	if strings.ToUpper(tok.Literal) != strings.ToUpper(lit) {
		return fmt.Errorf("lexer: expected %s but found %s", lit, tok.Literal)
	}
	return nil
}
