package minisql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"unicode/utf8"
)

var errJSONInvertedScannerFallback = errors.New("json inverted scanner fallback")

type jsonStringToken struct {
	value     string
	raw       []byte
	hasEscape bool
}

type jsonInvertedScanner struct {
	input []byte
	pos   int
	terms []string
}

func scanJSONInvertedTerms(input []byte, terms []string) ([]string, error) {
	scanner := jsonInvertedScanner{
		input: input,
		terms: terms,
	}
	if err := scanner.parseValue(""); err != nil {
		return nil, err
	}
	scanner.skipSpace()
	if scanner.pos != len(scanner.input) {
		return nil, fmt.Errorf("invalid character %q after top-level JSON value", scanner.input[scanner.pos])
	}
	slices.Sort(scanner.terms)
	return slices.Compact(scanner.terms), nil
}

func (s *jsonInvertedScanner) parseValue(path string) error {
	s.skipSpace()
	if s.pos >= len(s.input) {
		return errors.New("unexpected end of JSON input")
	}
	switch s.input[s.pos] {
	case '{':
		return s.parseObject(path)
	case '[':
		return s.parseArray(path)
	case '"':
		token, err := s.parseStringValue()
		if err != nil {
			return err
		}
		if path == "" {
			path = "$"
		}
		s.appendStringTerm(path, token)
		return nil
	case 't':
		if !s.consumeLiteral("true") {
			return s.invalidValue()
		}
		if path == "" {
			path = "$"
		}
		appendJSONInvertedScalarTerm(&s.terms, path, true)
		return nil
	case 'f':
		if !s.consumeLiteral("false") {
			return s.invalidValue()
		}
		if path == "" {
			path = "$"
		}
		appendJSONInvertedScalarTerm(&s.terms, path, false)
		return nil
	case 'n':
		if !s.consumeLiteral("null") {
			return s.invalidValue()
		}
		if path == "" {
			path = "$"
		}
		appendJSONInvertedScalarTerm(&s.terms, path, nil)
		return nil
	default:
		if s.input[s.pos] == '-' || isJSONDigit(s.input[s.pos]) {
			return s.parseNumberValue(path)
		}
		return s.invalidValue()
	}
}

func (s *jsonInvertedScanner) parseObject(path string) error {
	s.pos += 1
	s.skipSpace()
	if s.consumeByte('}') {
		return nil
	}

	var inlineKeys [8]string
	keys := inlineKeys[:0]
	var extraKeys []string
	for {
		s.skipSpace()
		if s.pos >= len(s.input) || s.input[s.pos] != '"' {
			return fmt.Errorf("expected object key at byte %d", s.pos)
		}
		key, err := s.parseStringKey()
		if err != nil {
			return err
		}
		if seenJSONInvertedObjectKey(keys, extraKeys, key.value) {
			return errJSONInvertedScannerFallback
		}
		if len(keys) < cap(keys) {
			keys = append(keys, key.value)
		} else {
			extraKeys = append(extraKeys, key.value)
		}

		s.skipSpace()
		if !s.consumeByte(':') {
			return fmt.Errorf("expected ':' after object key at byte %d", s.pos)
		}

		childPath := joinJSONInvertedPath(path, key.value)
		appendJSONInvertedTerm(&s.terms, "k:"+childPath)
		if err := s.parseValue(childPath); err != nil {
			return err
		}

		s.skipSpace()
		if s.consumeByte('}') {
			return nil
		}
		if !s.consumeByte(',') {
			return fmt.Errorf("expected ',' or '}' at byte %d", s.pos)
		}
	}
}

func (s *jsonInvertedScanner) parseArray(path string) error {
	s.pos += 1
	s.skipSpace()
	if s.consumeByte(']') {
		return nil
	}

	arrayPath := path + "[]"
	for {
		if err := s.parseValue(arrayPath); err != nil {
			return err
		}
		s.skipSpace()
		if s.consumeByte(']') {
			return nil
		}
		if !s.consumeByte(',') {
			return fmt.Errorf("expected ',' or ']' at byte %d", s.pos)
		}
	}
}

func (s *jsonInvertedScanner) parseNumberValue(path string) error {
	start := s.pos
	if err := s.scanNumber(); err != nil {
		return err
	}
	if path == "" {
		path = "$"
	}
	term := make([]byte, 0, len("kv:")+len(path)+len(":n:")+s.pos-start)
	term = append(term, "kv:"...)
	term = append(term, path...)
	term = append(term, ":n:"...)
	term = append(term, canonicalJSONNumber(jsonNumberFromBytes(s.input[start:s.pos]))...)
	if len(term) <= MaxIndexKeySize {
		s.terms = append(s.terms, string(term))
	}
	return nil
}

func (s *jsonInvertedScanner) parseStringKey() (jsonStringToken, error) {
	return s.parseString(true)
}

func (s *jsonInvertedScanner) parseStringValue() (jsonStringToken, error) {
	return s.parseString(false)
}

func (s *jsonInvertedScanner) parseString(needValue bool) (jsonStringToken, error) {
	if !s.consumeByte('"') {
		return jsonStringToken{}, fmt.Errorf("expected string at byte %d", s.pos)
	}
	rawStart := s.pos
	hasEscape := false
	for s.pos < len(s.input) {
		c := s.input[s.pos]
		switch {
		case c == '"':
			raw := s.input[rawStart:s.pos]
			s.pos += 1
			if !hasEscape {
				token := jsonStringToken{raw: raw}
				if needValue {
					token.value = string(raw)
				}
				return token, nil
			}
			decoded, err := strconv.Unquote(string(s.input[rawStart-1 : s.pos]))
			if err != nil {
				return jsonStringToken{}, err
			}
			return jsonStringToken{value: decoded, raw: raw, hasEscape: true}, nil
		case c == '\\':
			hasEscape = true
			s.pos += 1
			if s.pos >= len(s.input) {
				return jsonStringToken{}, errors.New("invalid string escape at end of JSON input")
			}
			esc := s.input[s.pos]
			if !isJSONEscape(esc) {
				return jsonStringToken{}, fmt.Errorf("invalid string escape %q at byte %d", esc, s.pos)
			}
			if esc == 'u' {
				for range 4 {
					s.pos += 1
					if s.pos >= len(s.input) || !isJSONHex(s.input[s.pos]) {
						return jsonStringToken{}, fmt.Errorf("invalid unicode escape at byte %d", s.pos)
					}
				}
			}
			s.pos += 1
		case c < 0x20:
			return jsonStringToken{}, fmt.Errorf("invalid control character in string at byte %d", s.pos)
		case c < utf8.RuneSelf:
			s.pos += 1
		default:
			r, size := utf8.DecodeRune(s.input[s.pos:])
			if r == utf8.RuneError && size == 1 {
				return jsonStringToken{}, fmt.Errorf("invalid utf-8 in string at byte %d", s.pos)
			}
			s.pos += size
		}
	}
	return jsonStringToken{}, errors.New("unexpected end of JSON string")
}

func (s *jsonInvertedScanner) appendStringTerm(path string, token jsonStringToken) {
	if token.hasEscape {
		appendJSONInvertedScalarTerm(&s.terms, path, token.value)
		return
	}
	term := make([]byte, 0, len("kv:")+len(path)+len(`:s:""`)+len(token.raw))
	term = append(term, "kv:"...)
	term = append(term, path...)
	term = append(term, `:s:"`...)
	term = append(term, token.raw...)
	term = append(term, '"')
	if len(term) <= MaxIndexKeySize {
		s.terms = append(s.terms, string(term))
	}
}

func (s *jsonInvertedScanner) scanNumber() error {
	start := s.pos
	if s.consumeByte('-') && s.pos >= len(s.input) {
		return errors.New("invalid JSON number")
	}
	switch {
	case s.consumeByte('0'):
		if s.pos < len(s.input) && isJSONDigit(s.input[s.pos]) {
			return fmt.Errorf("invalid leading zero in JSON number at byte %d", start)
		}
	case isJSONDigitOneToNine(s.peekByte()):
		for isJSONDigit(s.peekByte()) {
			s.pos += 1
		}
	default:
		return fmt.Errorf("invalid JSON number at byte %d", start)
	}
	if s.consumeByte('.') {
		if !isJSONDigit(s.peekByte()) {
			return fmt.Errorf("invalid JSON number fraction at byte %d", start)
		}
		for isJSONDigit(s.peekByte()) {
			s.pos += 1
		}
	}
	if s.peekByte() == 'e' || s.peekByte() == 'E' {
		s.pos += 1
		if s.peekByte() == '+' || s.peekByte() == '-' {
			s.pos += 1
		}
		if !isJSONDigit(s.peekByte()) {
			return fmt.Errorf("invalid JSON number exponent at byte %d", start)
		}
		for isJSONDigit(s.peekByte()) {
			s.pos += 1
		}
	}
	return nil
}

func (s *jsonInvertedScanner) consumeLiteral(literal string) bool {
	if !bytes.HasPrefix(s.input[s.pos:], []byte(literal)) {
		return false
	}
	s.pos += len(literal)
	return true
}

func (s *jsonInvertedScanner) skipSpace() {
	for s.pos < len(s.input) {
		switch s.input[s.pos] {
		case ' ', '\n', '\r', '\t':
			s.pos += 1
		default:
			return
		}
	}
}

func (s *jsonInvertedScanner) consumeByte(c byte) bool {
	if s.pos >= len(s.input) || s.input[s.pos] != c {
		return false
	}
	s.pos += 1
	return true
}

func (s *jsonInvertedScanner) peekByte() byte {
	if s.pos >= len(s.input) {
		return 0
	}
	return s.input[s.pos]
}

func (s *jsonInvertedScanner) invalidValue() error {
	if s.pos >= len(s.input) {
		return errors.New("unexpected end of JSON input")
	}
	return fmt.Errorf("invalid JSON value at byte %d", s.pos)
}

func seenJSONInvertedObjectKey(keys, extraKeys []string, key string) bool {
	for _, seen := range keys {
		if seen == key {
			return true
		}
	}
	for _, seen := range extraKeys {
		if seen == key {
			return true
		}
	}
	return false
}

func jsonNumberFromBytes(data []byte) json.Number {
	return json.Number(string(data))
}

func isJSONDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isJSONDigitOneToNine(c byte) bool {
	return c >= '1' && c <= '9'
}

func isJSONHex(c byte) bool {
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F')
}

func isJSONEscape(c byte) bool {
	switch c {
	case '"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u':
		return true
	default:
		return false
	}
}
