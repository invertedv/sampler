package sampler

import (
	"fmt"
	"os"
	"testing"

	"github.com/invertedv/chutils"
	"github.com/stretchr/testify/assert"
)

// tests assume a connection to the DB produced by github.com/invertedv/freddie
func TestStrat_Make(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	host := os.Getenv("host")
	conn, e := chutils.NewConnect(host, user, pw, nil)
	assert.Nil(t, e)
	strt := NewStrat("SELECT purpose, state, rate, fpDt FROM mtg.freddie", conn, true)
	strt.MinCount(10000)

	e = strt.Make("purpose", "fpDt")
	assert.Nil(t, e)
	fmt.Println(strt)
}

func TestGenerator_SampleRates(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	host := os.Getenv("host")
	conn, e := chutils.NewConnect(host, user, pw, nil)
	assert.Nil(t, e)

	gen := &Generator{
		conn:        conn,
		query:       "SELECT fico, purpose FROM mtg.freddie WHERE purpose!='U'",
		sampleTable: "",
		targetTotal: 2000000,
		sampleCap:   0.5,
		minCount:    0,
	}
	e = gen.CalcRates("purpose")
	assert.Nil(t, e)
	fmt.Println(gen)
}

func TestGenerator_MakeTable(t *testing.T) {
	user := os.Getenv("user")
	pw := os.Getenv("pw")
	host := os.Getenv("host")
	conn, e := chutils.NewConnect(host, user, pw, nil)
	assert.Nil(t, e)
	gen := NewGenerator("SELECT lnId, fico, state, purpose, servicer, fpDt FROM mtg.freddieNew",
		"tmp.test1",
		"tmp.test",
		200000,
		true,
		conn)
	gen.MinCount(10000)
	e = gen.CalcRates("state", "purpose")
	assert.Nil(t, e)
	e = gen.MakeTable()
	assert.Nil(t, e)
	fmt.Println(gen)
	e = gen.Strats().Plot("", true)
	assert.Nil(t, e)
	e = gen.SampleStrats().Plot("", true)
	assert.Nil(t, e)
}
