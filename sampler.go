package sampler

import (
	"fmt"
	"github.com/invertedv/chutils"

	s "github.com/invertedv/chutils/sql"
)

type Generator struct {
	conn        *chutils.Connect
	BaseQuery   string
	StratFields []string
	OutputTable string
	Limit       int
}

type Strat map[any]int

func (gn *Generator) GetStrat(field string) (Strat, error) {
	qry := fmt.Sprintf("SELECT toString(%s) AS grp, count(*) AS n FROM (%s) GROUP BY grp ORDER BY grp", field, gn.BaseQuery)
	rdr := s.NewReader(qry, gn.conn)
	if e := rdr.Init("grp", chutils.MergeTree); e != nil {
		return nil, e
	}
	strat := make(Strat)
	var (
		grp string
		n   int
	)
	rows, _, e := rdr.Read(0, false)
	if e != nil {
		return nil, e
	}
	for ind := 0; ind < len(rows); ind++ {
		grp = rows[ind][0].(string)
		n = rows[ind][1].(int)
		strat[grp] = n
	}
	return strat, nil

}
