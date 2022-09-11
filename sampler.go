package sampler

// TODO: don't let strat on float
import (
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/invertedv/chutils"

	s "github.com/invertedv/chutils/sql"
)

type Strat struct {
	Fields     []string  // field names from which to form strats
	Keys       [][]any   // each element is a combination of strat values
	Count      []uint64  // count of rows with Keys from corresponding slice element
	SampleRate []float64 // sample rates to achieve a stratified sample of a given size
	sortField  string    // field to sort strats on for printing
	sortAscend bool
}

func NewStrat(fields ...string) *Strat {
	keys := make([][]any, 0)
	counts := make([]uint64, 0)
	rates := make([]float64, 0)
	return &Strat{
		Fields:     fields,
		Keys:       keys,
		Count:      counts,
		SampleRate: rates,
		sortField:  "",
		sortAscend: false,
	}
}

func (str *Strat) addRow(fieldVals chutils.Row) error {
	if len(fieldVals)-1 != len(str.Fields) {
		return fmt.Errorf("(*Strat) addRow: field count of %d and return row of %d elements", len(fieldVals)-1, len(str.Fields))
	}
	val := make([]any, len(str.Fields))
	for ind := 0; ind < len(str.Fields); ind++ {
		val[ind] = fieldVals[ind]
	}
	str.Keys = append(str.Keys, val)
	str.Count = append(str.Count, fieldVals[len(fieldVals)-1].(uint64))
	return nil
}

// TODO: export this from chutils
// writeElement writes a single field with separator char. For strings, the text qualifier is sdelim.
// If sdelim is found, it is doubled.
func writeElement(el interface{}, char string, sdelim string) []byte {
	if el == nil {
		return []byte(fmt.Sprintf("array()%s", char))
	}
	switch v := el.(type) {
	case string:
		if strings.Contains(v, sdelim) {
			return []byte(fmt.Sprintf("'%s'%s", strings.Replace(v, sdelim, sdelim+sdelim, -1), char))
		}
		return []byte(fmt.Sprintf("'%s'%s", v, char))
	case time.Time:
		return []byte(fmt.Sprintf("'%s'%s", v.Format("2006-01-02"), char))
	case float64, float32:
		return []byte(fmt.Sprintf("%v%s", v, char))
	default:
		return []byte(fmt.Sprintf("%v%s", v, char))
	}
}

func (str *Strat) Save(table string, conn *chutils.Connect) error {
	const sep = ","

	if len(str.Keys) == 0 {
		return fmt.Errorf("(*Strat)Save: cannot save empty strats")
	}

	// build TableDef of output table
	fds := make(map[int]*chutils.FieldDef)

	for ind := 0; ind < len(str.Fields); ind++ {
		ch := chutils.ChField{}
		switch str.Keys[0][ind].(type) {
		case int32:
			ch.Base, ch.Length = chutils.ChInt, 32
		case int64:
			ch.Base, ch.Length = chutils.ChInt, 64
		case string:
			ch.Base = chutils.ChString
		default:
			return fmt.Errorf("unsupported type")
		}
		fd := chutils.NewFieldDef(str.Fields[ind], ch, "", nil, nil, 0)
		fds[ind] = fd
	}
	n := len(str.Fields)

	fd := chutils.NewFieldDef("count", chutils.ChField{Base: chutils.ChInt, Length: 32}, "", nil, nil, 0)
	fds[n] = fd

	if len(str.SampleRate) > 0 {
		n++
		fd = chutils.NewFieldDef("sampleRate", chutils.ChField{Base: chutils.ChFloat, Length: 64}, "", nil, nil, 0)
		fds[n] = fd
	}

	td := chutils.NewTableDef(str.Fields[0], chutils.MergeTree, fds)

	if e := td.Create(conn, table); e != nil {
		return e
	}

	wtr := s.NewWriter(table, conn)

	for row := 0; row < len(str.Count); row++ {
		line := make([]byte, 0)

		for col := 0; col < len(str.Fields); col++ {
			line = append(line, writeElement(str.Keys[row][col], sep, wtr.Text())...)
		}
		line = append(line, writeElement(str.Count[row], sep, wtr.Text())...)
		if len(str.SampleRate) > 0 {
			line = append(line, writeElement(str.SampleRate[row], sep, wtr.Text())...)
		}
		char := byte(' ')
		if wtr.EOL() != 0 {
			char = byte(wtr.EOL())
		}
		line[len(line)-1] = char

		if _, e := wtr.Write(line); e != nil {
			return e
		}
	}

	if e := wtr.Insert(); e != nil {
		return e
	}

	return nil
}

// TODO: make a 'remaining' category if have too many categories
func (str *Strat) String() string {
	const (
		spaces  = 4
		maxShow = 100
	)
	if str == nil {
		return ""
	}
	if len(str.Keys) == 0 {
		return ""
	}
	maxes := make([]int, len(str.Fields))
	maxCnt := spaces // max width of count field
	for row := 0; row < len(str.Count); row++ {
		for col := 0; col < len(str.Fields); col++ {
			if row == 0 {
				maxes[col] = spaces + len(str.Fields[col])
			}
			keyStr := fmt.Sprintf("%v", str.Keys[row][col])
			maxes[col] = Max(maxes[col], len(keyStr)+spaces)
		}
		maxCnt = Max(maxCnt, len(humanize.Comma(int64(str.Count[row]))))
	}

	strg := ""
	for col := 0; col < len(str.Fields); col++ {
		strg = fmt.Sprintf("%s%s", strg, padder(str.Fields[col], maxes[col], true))
	}

	strg += padder("Count", maxCnt+spaces, true)

	var maxCapt int
	if len(str.SampleRate) > 0 {
		strg += padder("Sample Rate", 11+spaces, true)
		strg += "Captured"
		maxCapt = len(humanize.Comma(int64(str.SampleRate[0] * float64(str.Count[0])))) // this is the biggest value
	}

	strg = strg + "\n"

	for row := 0; row < Min(maxShow, len(str.Count)); row++ {
		for col := 0; col < len(str.Fields); col++ {
			keyStr := fmt.Sprintf("%v", str.Keys[row][col])
			strg = fmt.Sprintf("%s%s", strg, padder(keyStr, maxes[col], true))
		}
		// pre-pend spaces so the RHS lines up
		strg = fmt.Sprintf("%s%s", strg, padder(padder(humanize.Comma(int64(str.Count[row])), maxCnt, false), maxCnt+spaces, true))
		if len(str.SampleRate) > 0 {
			//pre-pend so RHS lines up
			strg = fmt.Sprintf("%s%s", strg, padder(padder(fmt.Sprintf("%0.2f%%", 100*str.SampleRate[row]), 6, false), 11+spaces, true))
			captured := int64(str.SampleRate[row] * float64(str.Count[row]))
			strg = fmt.Sprintf("%s%s", strg, padder(humanize.Comma(captured), maxCapt, false))
		}
		strg = strg + "\n"
	}

	return strg

}

type GenOpt func(gn *Generator)

type Generator struct {
	conn        *chutils.Connect
	baseQuery   string
	stratFields []string
	outputTable string
	stratTable  string
	targetTotal int
	minCount    uint64
	sampleCap   float64
	n           int
	strats      *Strat
	captured    int
	makeQuery   string
}

func NewGenerator(baseQuery, outputTable, stratTable string, targetTotal int, conn *chutils.Connect, stratFields ...string) *Generator {
	return &Generator{
		conn:        conn,
		baseQuery:   baseQuery,
		stratFields: stratFields,
		outputTable: outputTable,
		stratTable:  stratTable,
		targetTotal: targetTotal,
		minCount:    0,
		sampleCap:   1.0,
		n:           0,
		strats:      nil,
		captured:    0,
		makeQuery:   "",
	}
}

func (gn *Generator) reset() {
	gn.n, gn.strats, gn.captured = 0, nil, 0
}

func (gn *Generator) SetMinCount(mc int) {
	gn.minCount = uint64(mc)
	gn.reset()
}

func (gn *Generator) SetSampleCap(cap float64) {
	gn.sampleCap = cap
	gn.reset()
}

func (gn *Generator) GetStrat(fields ...string) (*Strat, error) {
	fieldsList := strings.Join(fields, ",")
	qry := fmt.Sprintf("SELECT %s, count(*) AS n FROM (%s) GROUP BY %s", fieldsList, gn.baseQuery, fieldsList)

	if gn.minCount > 0 {
		qry = fmt.Sprintf("%s HAVING n >= %d", qry, gn.minCount)
	}
	qry = fmt.Sprintf("%s %s", qry, "ORDER BY n DESC")

	rdr := s.NewReader(qry, gn.conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return nil, e
	}

	strat := NewStrat(fields...)

	rows, _, e := rdr.Read(0, false)
	if e != nil {
		return nil, e
	}

	for ind := 0; ind < len(rows); ind++ {
		if e := strat.addRow(rows[ind]); e != nil {
			return nil, e
		}
	}

	return strat, nil

}

func (gn *Generator) SampleRates() error {
	const (
		maxIter = 5
		tol     = 0.01
	)

	var e error
	gn.strats, e = gn.GetStrat(gn.stratFields...)
	if e != nil {
		return e
	}

	tot := 0
	for _, c := range gn.strats.Count {
		tot += int(c)
	}

	gn.n = tot

	gn.strats.SampleRate = make([]float64, len(gn.strats.Count))
	iter := true

	target := gn.targetTotal
	free := len(gn.strats.Count)
	iterCount := 0
	capturedObs := 0
	tolerance := int(tol * float64(gn.targetTotal)) // call it good if we've gotten within this many obs of target
	for iter {
		lostObs := 0
		perStrat := float64(target) / float64(free)
		fmt.Println(" perStrat: ", perStrat, "# strats: ", len(gn.strats.Count), int(perStrat)*len(gn.strats.Count))

		for ind, c := range gn.strats.Count {
			if gn.strats.SampleRate[ind] >= 1.0 {
				continue
			}

			rate := perStrat / float64(c)

			// taking more than allowed?
			if rate+gn.strats.SampleRate[ind] > gn.sampleCap {
				rate = gn.sampleCap - gn.strats.SampleRate[ind]
			}

			capturedObs += int(rate * float64(c))
			gn.strats.SampleRate[ind] += rate

			if float64(c) < perStrat {
				lostObs += int(perStrat) - int(c)
				free--
			}
		}

		target = gn.targetTotal - capturedObs
		fmt.Println("new target: ", target)
		iterCount++

		fmt.Println("free: ", free, " lost: ", lostObs, "# strats: ", len(gn.strats.Count), " captured: ", capturedObs)
		fmt.Println(capturedObs + lostObs)
		fmt.Println(gn.strats)
		iter = iterCount < maxIter && lostObs > tolerance && free > 0
	}
	gn.captured = capturedObs

	return nil
}

func (gn *Generator) MakeTable() error {

	gn.strats.Save(gn.stratTable, gn.conn)
	qry := fmt.Sprintf("SELECT\n  a.*\nFROM\n  (%s) AS a\nJOIN\n  %s AS b\n ON \n", gn.baseQuery, gn.stratTable)
	joins := make([]string, 0)
	for _, f := range gn.stratFields {
		joins = append(joins, fmt.Sprintf("a.%s = b.%s\n", f, f))
	}

	qry = fmt.Sprintf("%s %s", qry, strings.Join(joins, " AND "))
	qry = fmt.Sprintf("%s WHERE rand32(1001) / 4294967295.0 < b.sampleRate\n", qry)
	fmt.Println(qry)
	rdr := s.NewReader(qry, gn.conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return e
	}

	if e := rdr.TableSpec().Create(gn.conn, gn.outputTable); e != nil {
		return e
	}

	rdr.Name = gn.outputTable

	if e := rdr.Insert(); e != nil {
		return e
	}

	return nil
}

func padder(s string, padTo int, appendTo bool) string {
	upper := padTo - len(s)
	for ind := 0; ind < upper; ind++ {
		if appendTo {
			s += " "
		} else {
			s = " " + s
		}
	}
	return s
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
