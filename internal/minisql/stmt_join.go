package minisql

import (
	"errors"
	"fmt"
)

// JoinType identifies the variety of JOIN operation to perform.
type JoinType int

// JoinType constants enumerate the supported JOIN varieties.
const (
	// Inner is an INNER JOIN — only rows that have a match in both tables.
	Inner JoinType = iota + 1
	// Left is a LEFT JOIN.
	Left
	// Right is a RIGHT JOIN.
	Right
	// FullOuter is a FULL OUTER JOIN — all rows from both tables, with NULLs where there is no match.
	FullOuter
	// Semi is a semi-join: emit the outer row when at least one matching inner row exists.
	// Used internally to implement IN (subquery) without materialising the subquery.
	Semi
	// AntiSemi is an anti-semi-join: emit the outer row when no matching inner row exists.
	// Used internally to implement NOT IN (subquery).
	AntiSemi
)

// Join describes a single JOIN clause within a SELECT statement, including the
// joined table, its alias, the ON conditions, any nested sub-joins, and the type.
type Join struct {
	TableName  string
	TableAlias string
	Conditions Conditions
	Joins      []Join
	Type       JoinType
}

// AddJoin attaches a new JOIN clause to the statement at the correct position in
// the join tree. fromTableAlias must match an existing alias (the base table or a
// previously added join), and toTableAlias must be unique. Returns an error if the
// statement is not a SELECT or the alias cannot be found.
func (s Statement) AddJoin(joinType JoinType, fromTableAlias, toTable, toTableAlias string, conditions Conditions) (Statement, error) {
	if s.Kind != Select {
		return Statement{}, errors.New("joins can only be added to SELECT statements")
	}

	if s.TableAlias == "" {
		return Statement{}, errors.New("cannot add join to statement without table alias")
	}

	if toTable == "" {
		return Statement{}, errors.New("toTable cannot be empty")
	}

	if toTableAlias == "" {
		return Statement{}, errors.New("toTableAlias cannot be empty")
	}

	if len(s.Joins) == 0 {
		if fromTableAlias != s.TableAlias {
			return Statement{}, fmt.Errorf("join from table alias %q does not match statement table alias %q", fromTableAlias, s.TableAlias)
		}
		s.Joins = append(s.Joins, Join{
			Type:       joinType,
			TableName:  toTable,
			TableAlias: toTableAlias,
			Conditions: conditions,
		})
		return s, nil
	}

	if fromTableAlias == s.TableAlias {
		s.Joins = append(s.Joins, Join{
			Type:       joinType,
			TableName:  toTable,
			TableAlias: toTableAlias,
			Conditions: conditions,
		})
		return s, nil
	}

	for i := range s.Joins {
		updatedJoin, added, err := s.Joins[i].addJoin(joinType, fromTableAlias, toTable, toTableAlias, conditions)
		if err != nil {
			return Statement{}, err
		}
		if added {
			s.Joins[i] = updatedJoin
			return s, nil
		}
	}

	return s, fmt.Errorf("could not find from table alias %q in existing joins", fromTableAlias)
}

func (j Join) addJoin(joinType JoinType, fromTableAlias, toTable, toTaableAlias string, conditions Conditions) (Join, bool, error) {
	if len(j.Joins) == 0 {
		if fromTableAlias != j.TableAlias {
			return Join{}, false, nil
		}
		j.Joins = append(j.Joins, Join{
			Type:       joinType,
			TableName:  toTable,
			TableAlias: toTaableAlias,
			Conditions: conditions,
		})
		return j, true, nil
	}

	if fromTableAlias == j.TableAlias {
		j.Joins = append(j.Joins, Join{
			Type:       joinType,
			TableName:  toTable,
			TableAlias: toTaableAlias,
			Conditions: conditions,
		})
		return j, true, nil
	}

	for i := range j.Joins {
		updatedJoin, added, err := j.Joins[i].addJoin(joinType, fromTableAlias, toTable, toTaableAlias, conditions)
		if err != nil {
			return Join{}, false, err
		}
		if added {
			j.Joins[i] = updatedJoin
			return j, true, nil
		}
	}
	return Join{}, false, nil
}

// FromTableAlias returns the alias of the table on the left-hand side of the
// JOIN condition by inspecting the first condition operand that belongs to a
// different alias than the join's own table alias. Returns "" if undetermined.
func (j Join) FromTableAlias() string {
	if len(j.Conditions) == 0 || j.TableAlias == "" {
		return ""
	}
	if j.Conditions[0].Operand1.Value.(Field).AliasPrefix != "" && j.Conditions[0].Operand1.Value.(Field).AliasPrefix != j.TableAlias {
		return j.Conditions[0].Operand1.Value.(Field).AliasPrefix
	}
	if j.Conditions[0].Operand2.Value.(Field).AliasPrefix != "" && j.Conditions[0].Operand2.Value.(Field).AliasPrefix != j.TableAlias {
		return j.Conditions[0].Operand2.Value.(Field).AliasPrefix
	}
	return ""
}
