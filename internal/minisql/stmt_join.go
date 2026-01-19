package minisql

import (
	"fmt"
)

type JoinType int

const (
	Inner JoinType = iota + 1
	Left
	Right
)

type Join struct {
	Type       JoinType
	TableName  string
	TableAlias string
	Conditions Conditions
	Joins      []Join
}

func (s Statement) AddJoin(aType JoinType, fromTableAlias, toTable, toTableAlias string, conditions Conditions) (Statement, error) {
	if s.Kind != Select {
		return Statement{}, fmt.Errorf("joins can only be added to SELECT statements")
	}

	if s.TableAlias == "" {
		return Statement{}, fmt.Errorf("cannot add join to statement without table alias")
	}

	if toTable == "" {
		return Statement{}, fmt.Errorf("toTable cannot be empty")
	}

	if toTableAlias == "" {
		return Statement{}, fmt.Errorf("toTableAlias cannot be empty")
	}

	if len(s.Joins) == 0 {
		if fromTableAlias != s.TableAlias {
			return Statement{}, fmt.Errorf("join from table alias %q does not match statement table alias %q", fromTableAlias, s.TableAlias)
		}
		s.Joins = append(s.Joins, Join{
			Type:       aType,
			TableName:  toTable,
			TableAlias: toTableAlias,
			Conditions: conditions,
		})
		return s, nil
	}

	if fromTableAlias == s.TableAlias {
		s.Joins = append(s.Joins, Join{
			Type:       aType,
			TableName:  toTable,
			TableAlias: toTableAlias,
			Conditions: conditions,
		})
		return s, nil
	}

	for i := range s.Joins {
		updatedJoin, added, err := s.Joins[i].addJoin(aType, fromTableAlias, toTable, toTableAlias, conditions)
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

func (j Join) addJoin(aType JoinType, fromTableAlias, toTable, toTaableAlias string, conditions Conditions) (Join, bool, error) {
	if len(j.Joins) == 0 {
		if fromTableAlias != j.TableAlias {
			return Join{}, false, nil
		}
		j.Joins = append(j.Joins, Join{
			Type:       aType,
			TableName:  toTable,
			TableAlias: toTaableAlias,
			Conditions: conditions,
		})
		return j, true, nil
	}

	if fromTableAlias == j.TableAlias {
		j.Joins = append(j.Joins, Join{
			Type:       aType,
			TableName:  toTable,
			TableAlias: toTaableAlias,
			Conditions: conditions,
		})
		return j, true, nil
	}

	for i := range j.Joins {
		updatedJoin, added, err := j.Joins[i].addJoin(aType, fromTableAlias, toTable, toTaableAlias, conditions)
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
