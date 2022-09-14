// Package sampler produces strats and stratified samples.  The package works with ClickHouse tables.
//
// This package does two things:
//
//  1. Produces strats.  That is, it produces a table of row counts for each stratum of an input source. The strata are
//     defined by the user.
//  2. Creates an output table from the input source that is balanced along the desired strata.
//
// # Sampling Procedure
//
// The goal is to generate a sample with an equal number of rows for each stratum.  A sample rate for each stratum is
// determined to achieve this.  If each stratum has sufficient rows, then we're done.  To the extent some strata have
// insufficient rows, then the total sample will be short.  Also, the sample won't be exactly balanced.  To achieve
// balance one would have to do one of these:
//
//   - Target a sample per strata that is equal to the size of the smallest stratum;
//   - Resample strata with insufficient rows
//
// Practically, the first is not an option as often there will be a stratum with very few rows. The second is tantamount
// to up-weighting the small strata.  In that case, these observations may become influential observations.
//
// Instead of these, this package adopts the philosophy that an approximate balance goes a long way to reducing the
// leverage of huge strata which is typical in data. The sampling algorithm used by sampler is:
//
// 1. At the start
//   - desired sample = total sample desired / # of strata
//   - stratum sample rate = min(desired sample / stratum size, sample rate cap)
//   - stratum captured sample = stratum sample rate * stratum size
//   - stratum free observations = stratum size - stratum captured sample
//
// 2. Update
//   - total sample desired = total sample desired - total captured
//   - if this number is "small" stop.
//   - stratum desired sample = total sample desired / # of strata with free observations
//   - stratum sample rate = min(stratum desired sample + stratum previously captured sample / stratum size, sample rate cap)
//   - stratum captured sample = stratum sample rate * stratum size
//   - stratum free observations = stratum size - stratum captured sample
//
// Step 2 is repeated (iterations capped at 5) until the target sample size is achieved (within tolerance) or
// no strata have free observations.
package sampler

import (
	"fmt"
	"strings"
	"time"

	grob "github.com/MetalBlueberry/go-plotly/graph_objects"
	sf "github.com/invertedv/seafan"

	"github.com/dustin/go-humanize"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
)

// Strat produces stratifications.
type Strat struct {
	fields       []string // field names from which to form strats
	keys         [][]any  // each element is a combination of strat values
	count        []uint64 // count of rows with Keys from corresponding slice element
	query        string   // query to pull data for strats
	minCount     int      // lower bound of counts for a strat to be included
	sortByCounts bool     // if true, strats are sorted descending by count, o.w. sorted ascending by strat
	n            uint64
	conn         *chutils.Connect // DB connection
}

func NewStrat(query string, conn *chutils.Connect, sortByCounts bool) *Strat {
	return &Strat{
		query:        query,
		conn:         conn,
		sortByCounts: sortByCounts,
	}
}

// MinCount returns (and optionally sets) the minimum # of obs for a strat to be included
func (strt *Strat) MinCount(mc int) int {
	if mc >= 0 {
		strt.minCount = mc
	}

	return strt.minCount
}

// N returns the total number of observations in the strats.
// This does not include strats dropped if MinCount > 0.
func (strt *Strat) N() uint64 {
	return strt.n
}

// Fields returns the current set of strat fields
func (strt *Strat) Fields() []string {
	return strt.fields
}

// Table returns the elements of the strat table. The table is stored by row.
// Each row is a stratum.
func (strt *Strat) Table() (keys [][]any, counts []uint64) {
	return strt.keys, strt.count
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
	cnt := fieldVals[len(fieldVals)-1].(uint64)
	strt.count = append(strt.count, cnt)
	strt.n += cnt
	return nil
}

// Make generates the strat table for the list of fields.
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

	strt.n = 0
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
		return val.Format("2006-01-02")
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Plot plots the count of observations for each strat from sampleTable
func (strt *Strat) Plot(outFile string, show bool) error {
	x := make([]string, len(strt.count))
	keys := make([]string, len(strt.fields))
	for row, f := range strt.keys {
		for col := 0; col < len(f); col++ {
			keys[col] = fmt.Sprintf("%v", f[col])
		}
		x[row] = strings.Join(keys, ":")
	}
	tr := &grob.Bar{X: x, Y: strt.count, Type: grob.TraceTypeBar}
	fig := &grob.Fig{Data: grob.Traces{tr}}
	return sf.Plotter(fig, nil, &sf.PlotDef{
		Show:     show,
		Title:    "Observation Count By Stratum",
		XTitle:   "Stratum",
		YTitle:   "Counts",
		STitle:   "",
		Legend:   false,
		Height:   1200,
		Width:    1600,
		FileName: outFile,
	})
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
		str += "\n"
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
			str += "\n"
		}
	}

	if len(strt.count) > maxShow*2 {
		str = fmt.Sprintf("%s    %d rows not shown\n", str, len(strt.count)-2*maxShow)
	}

	str = fmt.Sprintf("%s    %d total obs", str, strt.n)

	return str
}

// Generator is used to produce stratified samples.
type Generator struct {
	// inputs
	query       string  // query to fetch data to sample
	sampleTable string  // table to create with sample
	stratTable  string  // table to create with strats/sampling rates
	targetTotal int     // total number of obs desired
	minCount    uint64  // minimum # of obs to include a strat in sample (default: 1)
	sampleCap   float64 // maximum sample rate for any strat (default: 0)
	sortByCount bool    // if true, sort strats descending by count

	// calculated fields
	sampleRate   []float64        // calculated sample rates to achieve a balanced sample
	strats       *Strat           // strats calculated from query data
	sampleStrats *Strat           // strats calculated from sampled data
	expCaptured  int              // expected size of sampleTable
	actCaptured  int              // actual size of sampleTable
	makeQuery    string           // query used to create sampleTable
	conn         *chutils.Connect // connection to DB
}

// NewGenerator returns a *Generator.
// query is the CH query to fetch the input data.
// sampleTable is the output table of the sampled input data.
// stratTable is the output table of strats & sampling rates of the input data.
// targetTotal is the target size of sampleTable
// sortByCount sorts strats by descending count, if true.  If false, the table is sorted by the strat fields.
func NewGenerator(query, sampleTable, stratTable string, targetTotal int, sortByCount bool, conn *chutils.Connect) *Generator {
	return &Generator{
		conn:        conn,
		query:       query,
		sampleTable: sampleTable,
		stratTable:  stratTable,
		targetTotal: targetTotal,
		sortByCount: sortByCount,
		minCount:    0,
		sampleCap:   1.0,
	}
}

// MakeQuery returns the query used to create sampleTable.
func (gn *Generator) MakeQuery() string {
	return gn.makeQuery
}

// Strats returns the strats of the input data.
func (gn *Generator) Strats() *Strat {
	return gn.strats
}

// SampleStrats returns the strats of sampleTable.
func (gn *Generator) SampleStrats() *Strat {
	return gn.sampleStrats
}

// MinCount returns (and optionally sets) the minimum # of obs for a strat to be sampled.
// The value is not updated if mc < 0.
func (gn *Generator) MinCount(mc int) uint64 {
	if mc <= 0 {
		return gn.minCount
	}
	gn.minCount = uint64(mc)
	gn.reset()
	return gn.minCount
}

// SampleCap returns (and optional sets) the maximum sampling rate for strats.
// The value is not updated if cap <= 0.0 or cap > 1.0
func (gn *Generator) SampleCap(sCap float64) float64 {
	if sCap <= 0.0 || sCap > 1.0 {
		return gn.sampleCap
	}

	gn.sampleCap = sCap
	gn.reset()

	return gn.sampleCap
}

// SampleRates returns the calculated sample rates. The slice is in the same order as Strats.
func (gn *Generator) SampleRates() []float64 {
	return gn.sampleRate
}

// CalcRates calculates the sampling rate for each strat to achieve a balanced sample with a total size of TargetTotal.
// fields is the set of fields to stratify on.
func (gn *Generator) CalcRates(fields ...string) error {
	const (
		maxIter = 5
		tol     = 0.01
	)

	if fields == nil {
		return fmt.Errorf("(*Generator) CalcRates: must specify strat fields")
	}
	gn.strats = NewStrat(gn.query, gn.conn, gn.sortByCount)
	gn.strats.MinCount(int(gn.minCount))

	if e := gn.strats.Make(fields...); e != nil {
		return e
	}

	tot := 0
	for _, c := range gn.strats.count {
		tot += int(c)
	}

	gn.sampleRate = make([]float64, len(gn.strats.count))
	iter := true

	target := gn.targetTotal     // target # of obs to capture
	free := len(gn.strats.count) // number of strata that have data available
	iterCount := 0
	capturedObs := 0                                // total obs we've captured toward the goal of gn.targetTotal
	tolerance := int(tol * float64(gn.targetTotal)) // call it good if we've gotten within this many obs of target

	// The approach is to calculate a target sample for each strat based on how many obs we need in total.
	// If there are enough in each stratum, this will take one try.  If some strata don't have enough obs,
	// then we increasingly draw for strata that still have data available.
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

// MakeTable creates sampleTable and stratTable.
func (gn *Generator) MakeTable() error {
	if gn.strats == nil {
		return fmt.Errorf("(*Generator) MakeTable: must run CalcRates first")
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
	gn.makeQuery = qry
	rdr := s.NewReader(qry, gn.conn)

	if e := rdr.Init("", chutils.MergeTree); e != nil {
		return e
	}

	if e := rdr.TableSpec().Create(gn.conn, gn.sampleTable); e != nil {
		return e
	}

	rdr.Name = gn.sampleTable

	if e := rdr.Insert(); e != nil {
		return e
	}

	qry = fmt.Sprintf("SELECT * FROM %s", gn.sampleTable)
	gn.sampleStrats = NewStrat(qry, gn.conn, gn.sortByCount)
	if e := gn.sampleStrats.Make(gn.strats.fields...); e != nil {
		return e
	}

	for ind := 0; ind < len(gn.sampleStrats.count); ind++ {
		gn.actCaptured += int(gn.sampleStrats.count[ind])
	}

	return nil
}

// Save saves stratTable to the DB.
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

// Marginals generates the strats of each field we're stratifying on.
func (gn *Generator) Marginals() ([]*Strat, string, error) {
	if gn.sampleStrats == nil {
		return nil, "", fmt.Errorf("(*Generator) Marginals: have not build sample table")
	}

	qry := fmt.Sprintf("SELECT * FROM %s", gn.sampleTable)
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
	str = fmt.Sprintf("%sStrats Table: %s\n", str, gn.stratTable)
	str = fmt.Sprintf("%sSample Table: %s\n", str, gn.sampleTable)
	str = fmt.Sprintf("%sMin Count: %s\n", str, humanize.Comma(int64(gn.minCount)))
	str = fmt.Sprintf("%sSampling Cap: %0.2f\n", str, gn.sampleCap)
	if gn.strats == nil {
		return str
	}
	str = fmt.Sprintf("%s\nTarget # Obs:%d\n", str, gn.targetTotal)
	str = fmt.Sprintf("%sExpected # Obs: %v", str, humanize.Comma(int64(gn.expCaptured)))
	if gn.sampleStrats != nil {
		str = fmt.Sprintf("%s\nActual # Obs: %v\n\nSample Table Strats\n", str, humanize.Comma(int64(gn.actCaptured)))
		str = fmt.Sprintf("%s%s", str, gn.sampleStrats.String())
		_, marg, e := gn.Marginals()
		if e != nil {
			str += "ERROR"
			return str
		}
		str = fmt.Sprintf("%s\n%s", str, marg)
	}

	str = fmt.Sprintf("%s\nInput Table Strats:\n", str)
	str = fmt.Sprintf("%s\n%s", str, gn.strats.String())

	return str
}

func (gn *Generator) reset() {
	gn.strats, gn.sampleStrats, gn.sampleRate, gn.expCaptured, gn.actCaptured, gn.makeQuery = nil, nil, nil, 0, 0, ""
}

func padder(inStr string, padTo int, appendTo bool) string {
	upper := padTo - len(inStr)
	for ind := 0; ind < upper; ind++ {
		if appendTo {
			inStr += " "
		} else {
			inStr = " " + inStr
		}
	}
	return inStr
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
