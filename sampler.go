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
	Fields      []string  // field names from which to form strats
	Keys        [][]any   // each element is a combination of strat values
	Count       []uint64  // count of rows with Keys from corresponding slice element
	SampleRate  []float64 // sample rates to achieve a stratified sample of a given size
	expCaptured []int
	actCaptured []int
	sortField   string // field to sort strats on for printing
	sortAscend  bool
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

func (strt *Strat) AddRow(fieldVals chutils.Row) error {
	if len(fieldVals)-1 != len(strt.Fields) {
		return fmt.Errorf("(*Strat) AddRow: field count of %d and return row of %d elements", len(fieldVals)-1, len(strt.Fields))
	}
	val := make([]any, len(strt.Fields))
	for ind := 0; ind < len(strt.Fields); ind++ {
		val[ind] = fieldVals[ind]
	}
	strt.Keys = append(strt.Keys, val)
	strt.Count = append(strt.Count, fieldVals[len(fieldVals)-1].(uint64))
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

func (strt *Strat) Save(table string, conn *chutils.Connect) error {
	const sep = ","

	if len(strt.Keys) == 0 {
		return fmt.Errorf("(*Strat)Save: cannot save empty strats")
	}

	// build TableDef of output table
	fds := make(map[int]*chutils.FieldDef)

	for ind := 0; ind < len(strt.Fields); ind++ {
		ch := chutils.ChField{}
		switch strt.Keys[0][ind].(type) {
		case int32:
			ch.Base, ch.Length = chutils.ChInt, 32
		case int64:
			ch.Base, ch.Length = chutils.ChInt, 64
		case string:
			ch.Base = chutils.ChString
		default:
			return fmt.Errorf("unsupported type")
		}
		fd := chutils.NewFieldDef(strt.Fields[ind], ch, "", nil, nil, 0)
		fds[ind] = fd
	}
	n := len(strt.Fields)

	fd := chutils.NewFieldDef("count", chutils.ChField{Base: chutils.ChInt, Length: 32}, "", nil, nil, 0)
	fds[n] = fd

	if len(strt.SampleRate) > 0 {
		n++
		fd = chutils.NewFieldDef("sampleRate", chutils.ChField{Base: chutils.ChFloat, Length: 64}, "", nil, nil, 0)
		fds[n] = fd
	}

	td := chutils.NewTableDef(strt.Fields[0], chutils.MergeTree, fds)

	if e := td.Create(conn, table); e != nil {
		return e
	}

	wtr := s.NewWriter(table, conn)

	for row := 0; row < len(strt.Count); row++ {
		line := make([]byte, 0)

		for col := 0; col < len(strt.Fields); col++ {
			line = append(line, writeElement(strt.Keys[row][col], sep, wtr.Text())...)
		}
		line = append(line, writeElement(strt.Count[row], sep, wtr.Text())...)
		if len(strt.SampleRate) > 0 {
			line = append(line, writeElement(strt.SampleRate[row], sep, wtr.Text())...)
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
func (strt *Strat) String() string {
	const (
		spaces  = 4
		maxShow = 100
	)
	if strt == nil {
		return ""
	}
	if len(strt.Keys) == 0 {
		return ""
	}
	maxes := make([]int, len(strt.Fields))
	maxCnt := spaces // max width of count field
	for row := 0; row < len(strt.Count); row++ {
		for col := 0; col < len(strt.Fields); col++ {
			if row == 0 {
				maxes[col] = spaces + len(strt.Fields[col])
			}
			keyStr := fmt.Sprintf("%v", strt.Keys[row][col])
			maxes[col] = Max(maxes[col], len(keyStr)+spaces)
		}
		maxCnt = Max(maxCnt, len(humanize.Comma(int64(strt.Count[row]))))
	}

	str := ""
	for col := 0; col < len(strt.Fields); col++ {
		str = fmt.Sprintf("%s%s", str, padder(strt.Fields[col], maxes[col], true))
	}

	str += padder("Count", maxCnt+spaces, true)

	var maxCapt int
	if len(strt.SampleRate) > 0 {
		str += padder("Sample Rate", 11+spaces, true)
		str += "Exp Captured"
		maxCapt = len(humanize.Comma(int64(strt.SampleRate[0] * float64(strt.Count[0])))) // this is the biggest value
	}

	str = str + "\n"

	for row := 0; row < Min(maxShow, len(strt.Count)); row++ {
		for col := 0; col < len(strt.Fields); col++ {
			keyStr := fmt.Sprintf("%v", strt.Keys[row][col])
			str = fmt.Sprintf("%s%s", str, padder(keyStr, maxes[col], true))
		}
		// pre-pend spaces so the RHS lines up
		str = fmt.Sprintf("%s%s", str, padder(padder(humanize.Comma(int64(strt.Count[row])), maxCnt, false), maxCnt+spaces, true))
		if len(strt.SampleRate) > 0 {
			//pre-pend so RHS lines up
			str = fmt.Sprintf("%s%s", str, padder(padder(fmt.Sprintf("%0.2f%%", 100*strt.SampleRate[row]), 6, false), 11+spaces, true))
			//			captured := int64(strt.SampleRate[row] * float64(strt.Count[row]))
			str = fmt.Sprintf("%s%s", str, padder(humanize.Comma(int64(strt.expCaptured[row])), maxCapt, false))
		}
		str = str + "\n"
	}

	return str

}

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

func (gn *Generator) Strats() *Strat {
	return gn.strats
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
		if e := strat.AddRow(rows[ind]); e != nil {
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
	gn.strats.expCaptured = make([]int, len(gn.strats.Count))
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

			gn.strats.expCaptured[ind] = int(rate * float64(c))
			capturedObs += gn.strats.expCaptured[ind]
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

	if e := gn.strats.Save(gn.stratTable, gn.conn); e != nil {
		return e
	}

	qry := fmt.Sprintf("SELECT\n  a.*\nFROM\n  (%s) AS a\nJOIN\n  %s AS b\n ON \n", gn.baseQuery, gn.stratTable)
	joins := make([]string, 0)

	for _, f := range gn.stratFields {
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

	return nil
}

// TODO: answer depends on whether sampled already
func (gn *Generator) String() string {

	str := "Settings:\n"
	str = fmt.Sprintf("%sTarget # Obs:%d\n", str, gn.targetTotal)
	str = fmt.Sprintf("%sStrats Table:%s\n", str, gn.stratTable)
	str = fmt.Sprintf("%sSample Table:%s\n", str, gn.outputTable)
	str = fmt.Sprintf("%sStrat Table:\n", str)
	str = fmt.Sprintf("%s\n%s", str, gn.strats.String())
	str = fmt.Sprintf("%s\n\nExpected # Obs: %v", str, humanize.Comma(int64(gn.captured)))

	return str
}

func (gn *Generator) reset() {
	gn.n, gn.strats, gn.captured = 0, nil, 0
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
