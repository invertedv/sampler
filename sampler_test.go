package sampler

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGenerator_GetStrat(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	conn, e := chutils.NewConnect("127.0.0.1", user, pw, nil)
	assert.Nil(t, e)

	gen := Generator{
		conn:        conn,
		baseQuery:   "SELECT purpose, state FROM mtg.fannieNew",
		stratFields: []string{"purpose", "state"},
		outputTable: "",
		minCount:    10000,
	}

	strat, e := gen.GetStrat("purpose")
	assert.Nil(t, e)
	fmt.Println(strat)
	e = strat.Save("tmp.test", conn)
	assert.Nil(t, e)

}

func TestGenerator_SampleRates(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	conn, e := chutils.NewConnect("127.0.0.1", user, pw, nil)
	assert.Nil(t, e)

	gen := Generator{
		conn:        conn,
		baseQuery:   "SELECT fico, purpose FROM mtg.fannieNew WHERE purpose!='U'",
		stratFields: []string{"purpose"},
		outputTable: "",
		targetTotal: 20000000,
		sampleCap:   0.5,
		minCount:    0,
	}
	e = gen.SampleRates()
	assert.Nil(t, e)
}

func TestGenerator_MakeTable(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	host := os.Getenv("host")
	conn, e := chutils.NewConnect(host, user, pw, nil)
	assert.Nil(t, e)
	gen := NewGenerator("SELECT lnId, fico, state, purpose FROM mtg.fannieNew",
		"tmp.test1",
		"tmp.test",
		200000,
		conn,
		"fico", "state")
	gen.SetMinCount(1000)
	e = gen.SampleRates()
	assert.Nil(t, e)
	e = gen.MakeTable()
	assert.Nil(t, e)

}
