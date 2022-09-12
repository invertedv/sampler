package sampler

// TODO: don't let strat on float
import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	"strings"
	"time"
)

type Strat struct {
	fields       []string // field names from which to form strats
	keys         [][]any  // each element is a combination of strat values
	count        []uint64 // count of rows with Keys from corresponding slice element
	query        string
	minCount     int
	conn         *chutils.Connect
	sortByCounts bool
}

func NewStrat(query string, conn *chutils.Connect, sortByCounts bool) *Strat {

	return &Strat{
		query:        query,
		conn:         conn,
		sortByCounts: sortByCounts,
	}
}

func (strt *Strat) MinCount(mc int) int {
	if mc >= 0 {
		strt.minCount = mc
	}

	return strt.minCount
}

func (strt *Strat) Fields() []string {
	return strt.fields
}

func (strt *Strat) addRow(fieldVals chutils.Row) error {
	if len(fieldVals)-1 != len(strt.fields) {
		return fmt.Errorf("(*Strat) addRow: field count of %d and return row of %d elements", len(fieldVals)-1, len(strt.fields))
	}
	val := make([]any, len(strt.fields))
	for ind := 0; ind < len(strt.fields); ind++ {
		val[ind] = fieldVals[ind]
	}
	strt.keys = append(strt.keys, val)
	strt.count = append(strt.count, fieldVals[len(fieldVals)-1].(uint64))
	return nil
}

func (strt *Strat) Make(fields ...string) error {

	strt.fields = fields
	strt.keys = nil
	strt.count = nil

	fieldsList := strings.Join(fields, ",")
	qry := fmt.Sprintf("SELECT %s, count(*) AS n FROM (%s) GROUP BY %s ", fieldsList, strt.query, fieldsList)

	if strt.minCount > 0 {
		qry = fmt.Sprintf("%s HAVING n >= %d", qry, strt.minCount)
	}
	switch strt.sortByCounts {
	case true:
		qry = fmt.Sprintf("%s ORDER BY n DESC", qry)
	case false:
		qry = fmt.Sprintf("%s ORDER BY %s", qry, fieldsList)
	}

	rdr := s.NewReader(qry, strt.conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return e
	}

	for _, fld := range fields {
		_, fd, e := rdr.TableSpec().Get(fld)
		if e != nil {
			return e
		}
		if fd.ChSpec.Base == chutils.ChFloat {
			return fmt.Errorf("cant stratify on type float: %s", fld)
		}
	}

	rows, _, e := rdr.Read(0, false)
	if e != nil {
		return e
	}

	for ind := 0; ind < len(rows); ind++ {
		if e := strt.addRow(rows[ind]); e != nil {
			return e
		}
	}

	return nil

}

func format(x any) string {
	switch val := x.(type) {
	case time.Time:
		return fmt.Sprintf("%v", val.Format("2006-01-02"))
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (strt *Strat) String() string {
	const (
		spaces  = 4
		maxShow = 10
	)

	if strt == nil {
		return ""
	}

	if len(strt.keys) == 0 {
		return ""
	}

	maxes := make([]int, len(strt.fields))
	maxCnt := spaces // max width of count field

	for row := 0; row < len(strt.count); row++ {
		for col := 0; col < len(strt.fields); col++ {
			if row == 0 {
				maxes[col] = spaces + len(strt.fields[col])
			}
			maxes[col] = Max(maxes[col], len(format(strt.keys[row][col]))+spaces)
		}
		maxCnt = Max(maxCnt, len(humanize.Comma(int64(strt.count[row]))))
	}

	// headers
	str := ""
	for col := 0; col < len(strt.fields); col++ {
		str = fmt.Sprintf("%s%s", str, padder(strt.fields[col], maxes[col], true))
	}
	str = fmt.Sprintf("%s%s\n", str, padder("Count", maxCnt+spaces, true))

	// first maxShow rows
	for row := 0; row < Min(maxShow, len(strt.count)); row++ {
		for col := 0; col < len(strt.fields); col++ {
			str = fmt.Sprintf("%s%s", str, padder(format(strt.keys[row][col]), maxes[col], true))
		}
		// pre-pend spaces so the RHS lines up
		str = fmt.Sprintf("%s%s", str, padder(padder(humanize.Comma(int64(strt.count[row])), maxCnt, false), maxCnt+spaces, true))
		str = str + "\n"
	}

	// last maxShow rows
	if len(strt.count) > maxShow {
		if len(strt.count) > 2*maxShow {
			str = fmt.Sprintf("%s    ....\n", str)
		}

		start := Max(len(strt.count)-maxShow, maxShow)
		finish := Min(len(strt.count), start+maxShow)
		for row := start; row < finish; row++ {
			for col := 0; col < len(strt.fields); col++ {
				str = fmt.Sprintf("%s%s", str, padder(format(strt.keys[row][col]), maxes[col], true))
			}
			// pre-pend spaces so the RHS lines up
			str = fmt.Sprintf("%s%s", str, padder(padder(humanize.Comma(int64(strt.count[row])), maxCnt, false), maxCnt+spaces, true))
			str = str + "\n"
		}

	}
	if len(strt.count) > maxShow*2 {
		str = fmt.Sprintf("%s    %d rows not shown", str, len(strt.count)-2*maxShow)
	}

	return str
}

type Generator struct {
	conn        *chutils.Connect
	query       string
	sampleRate  []float64
	outputTable string
	stratTable  string
	targetTotal int
	minCount    uint64
	sampleCap   float64
	n           int
	strats      *Strat
	actStrats   *Strat
	expCaptured int
	actCaptured int
	sortByCount bool
	makeQuery   string
}

func NewGenerator(query, outputTable, stratTable string, targetTotal int, sortByCount bool, conn *chutils.Connect) *Generator {
	return &Generator{
		conn:        conn,
		query:       query,
		outputTable: outputTable,
		stratTable:  stratTable,
		targetTotal: targetTotal,
		sortByCount: sortByCount,
		minCount:    0,
		sampleCap:   1.0,
	}
}

func (gn *Generator) Strats() *Strat {
	return gn.strats
}

func (gn *Generator) ActStrats() *Strat {
	return gn.actStrats
}

func (gn *Generator) MinCount(mc int) uint64 {
	if mc <= 0 {
		return gn.minCount
	}
	gn.minCount = uint64(mc)
	gn.reset()
	return gn.minCount
}

func (gn *Generator) SetSampleCap(cap float64) float64 {
	if cap == 0.0 {
		return gn.sampleCap
	}

	gn.sampleCap = cap
	gn.reset()

	return gn.sampleCap
}

func (gn *Generator) SampleRates(fields ...string) error {
	const (
		maxIter = 5
		tol     = 0.01
	)

	var e error
	if fields == nil {
		return fmt.Errorf("(*Generator) SampleRates: must specify strat fields")
	}
	gn.strats = NewStrat(gn.query, gn.conn, gn.sortByCount)

	if e := gn.strats.Make(fields...); e != nil {
		return e
	}

	if e != nil {
		return e
	}

	tot := 0
	for _, c := range gn.strats.count {
		tot += int(c)
	}

	gn.n = tot

	gn.sampleRate = make([]float64, len(gn.strats.count))
	iter := true

	target := gn.targetTotal
	free := len(gn.strats.count)
	iterCount := 0
	capturedObs := 0
	tolerance := int(tol * float64(gn.targetTotal)) // call it good if we've gotten within this many obs of target
	for iter {
		lostObs := 0
		perStrat := float64(target) / float64(free)

		for ind, c := range gn.strats.count {
			if gn.sampleRate[ind] >= 1.0 {
				continue
			}

			rate := perStrat / float64(c)

			// taking more than allowed?
			if rate+gn.sampleRate[ind] > gn.sampleCap {
				rate = gn.sampleCap - gn.sampleRate[ind]
			}

			capturedObs += int(rate * float64(c))
			gn.sampleRate[ind] += rate

			if float64(c) < perStrat {
				lostObs += int(perStrat) - int(c)
				free--
			}
		}

		target = gn.targetTotal - capturedObs
		iterCount++
		iter = iterCount < maxIter && lostObs > tolerance && free > 0
	}

	gn.expCaptured = capturedObs

	return nil
}

func (gn *Generator) MakeTable() error {

	if gn.strats == nil {
		return fmt.Errorf("(*Generator) MakeTable: must run SampleRates first")
	}

	if e := gn.Save(); e != nil {
		return e
	}

	qry := fmt.Sprintf("SELECT\n  a.*\nFROM\n  (%s) AS a\nJOIN\n  %s AS b\n ON \n", gn.query, gn.stratTable)
	joins := make([]string, 0)

	for _, f := range gn.strats.fields {
		joins = append(joins, fmt.Sprintf("a.%s = b.%s\n", f, f))
	}

	qry = fmt.Sprintf("%s %s", qry, strings.Join(joins, " AND "))
	qry = fmt.Sprintf("%s WHERE rand32(1001) / 4294967295.0 < b.sampleRate\n", qry)
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

	qry = fmt.Sprintf("SELECT * FROM %s", gn.outputTable)
	gn.actStrats = NewStrat(qry, gn.conn, gn.sortByCount)
	if e := gn.actStrats.Make(gn.strats.fields...); e != nil {
		return e
	}

	for ind := 0; ind < len(gn.actStrats.count); ind++ {
		gn.actCaptured += int(gn.actStrats.count[ind])
	}

	return nil
}

func (gn *Generator) Save() error {
	const sep = ","

	if len(gn.strats.keys) == 0 {
		return fmt.Errorf("(*Strat)Save: cannot save empty strats")
	}

	// build TableDef of output table
	fds := make(map[int]*chutils.FieldDef)

	for ind := 0; ind < len(gn.strats.fields); ind++ {
		ch := chutils.ChField{}
		switch gn.strats.keys[0][ind].(type) {
		case int32:
			ch.Base, ch.Length = chutils.ChInt, 32
		case int64:
			ch.Base, ch.Length = chutils.ChInt, 64
		case string:
			ch.Base = chutils.ChString
		case time.Time:
			ch.Base = chutils.ChDate
		default:
			return fmt.Errorf("unsupported type")
		}
		fd := chutils.NewFieldDef(gn.strats.fields[ind], ch, "", nil, nil, 0)
		fds[ind] = fd
	}
	n := len(gn.strats.fields)

	fd := chutils.NewFieldDef("count", chutils.ChField{Base: chutils.ChInt, Length: 32}, "", nil, nil, 0)
	fds[n] = fd

	if len(gn.sampleRate) > 0 {
		n++
		fd = chutils.NewFieldDef("sampleRate", chutils.ChField{Base: chutils.ChFloat, Length: 64}, "", nil, nil, 0)
		fds[n] = fd
	}

	td := chutils.NewTableDef(gn.strats.fields[0], chutils.MergeTree, fds)

	if e := td.Create(gn.conn, gn.stratTable); e != nil {
		return e
	}

	wtr := s.NewWriter(gn.stratTable, gn.conn)

	for row := 0; row < len(gn.strats.count); row++ {
		line := make([]byte, 0)

		for col := 0; col < len(gn.strats.fields); col++ {
			line = append(line, chutils.WriteElement(gn.strats.keys[row][col], sep, wtr.Text())...)
		}
		line = append(line, chutils.WriteElement(gn.strats.count[row], sep, wtr.Text())...)
		if len(gn.sampleRate) > 0 {
			line = append(line, chutils.WriteElement(gn.sampleRate[row], sep, wtr.Text())...)
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

func (gn *Generator) Marginals() ([]*Strat, string, error) {
	if gn.actStrats == nil {
		return nil, "", fmt.Errorf("(*Generator) Marginals: have not build sample table")
	}

	qry := fmt.Sprintf("SELECT * FROM %s", gn.outputTable)
	strats := make([]*Strat, 0)
	str := ""

	for _, f := range gn.strats.fields {
		actStrat := NewStrat(qry, gn.conn, gn.sortByCount)
		if e := actStrat.Make(f); e != nil {
			return nil, "", e
		}
		strats = append(strats, actStrat)
		str = fmt.Sprintf("%s\nMarginal Distribution of %s", str, f)
		str = fmt.Sprintf("%s\n%s\n", str, actStrat)
	}

	return strats, str, nil
}

func (gn *Generator) String() string {
	str := ""
	str = fmt.Sprintf("%sStrats Table:%s\n", str, gn.stratTable)
	str = fmt.Sprintf("%sSample Table:%s\n", str, gn.outputTable)
	if gn.strats == nil {
		return str
	}
	str = fmt.Sprintf("%s\nTarget # Obs:%d\n", str, gn.targetTotal)
	str = fmt.Sprintf("%sExpected # Obs: %v", str, humanize.Comma(int64(gn.expCaptured)))
	if gn.actStrats != nil {
		str = fmt.Sprintf("%s\nActual # Obs: %v\n\nSample Table Strats\n", str, humanize.Comma(int64(gn.actCaptured)))
		str = fmt.Sprintf("%s%s", str, gn.actStrats.String())
		_, marg, e := gn.Marginals()
		if e != nil {
			str += "ERROR"
			return str
		}
		str = fmt.Sprintf("%s\n%s", str, marg)
	}

	str = fmt.Sprintf("%s\nSource Strat Table:\n", str)
	str = fmt.Sprintf("%s\n%s", str, gn.strats.String())

	return str
}

func (gn *Generator) reset() {
	gn.n, gn.strats, gn.expCaptured = 0, nil, 0
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
