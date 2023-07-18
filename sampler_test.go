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
	strt := NewStrat("SELECT purpose, state, origRate, vintageDt FROM bk0.final", conn, true)
	strt.MinCount(10000)

	e = strt.Make("purpose", "vintageDt")
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
		query:       "SELECT origFico, purpose FROM bk0.final",
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
	gen := NewGenerator("SELECT lnID, origFico, state, purpose, vintageDt FROM bk0.final",
		"tmp.test1",
		"tmp.test",
		200000,
		true,
		conn)
	gen.MinCount(10000)
	e = gen.CalcRates("state", "purpose")
	assert.Nil(t, e)
	e = gen.MakeTable(60)
	assert.Nil(t, e)
	fmt.Println(gen)
	e = gen.Strats().Plot("", true)
	assert.Nil(t, e)
	e = gen.SampleStrats().Plot("", true)
	assert.Nil(t, e)
}
