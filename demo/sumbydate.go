package main

import (
	"aggregator"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	//"github.com/pkg/profile"
)

func main() {
	//defer profile.Start().Stop()

	runtime.GOMAXPROCS(runtime.NumCPU())

	agg := aggregator.AggContainer{
		PathToFiles: os.Args[1],
		HeaderRow: "date; A; B; C",
		FileExt: "cvs",
	}

	agg.SetMapper(aggMapper)
	agg.SetUnMapper(aggUnMapper)
	agg.SetReducer(aggReducer)

	err := agg.Init()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = agg.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(agg.ResultFile)
}

func aggMapper(row string) (string, []interface{}, error) {
	fieldsCount := 4
	delimiter := ";"

	s := strings.Split(row, delimiter)
	if len(s) != fieldsCount {
		return "", nil, errors.New("Bad size")
	}

	var interfaceSlice = make([]interface{}, fieldsCount-1)
	a, _ := strconv.ParseFloat(strings.TrimSpace(s[1]), 64)
	interfaceSlice[0] = a
	b, _ := strconv.ParseFloat(strings.TrimSpace(s[2]), 64)
	interfaceSlice[1] = b
	c, _ := strconv.ParseFloat(strings.TrimSpace(s[3]), 64)
	interfaceSlice[2] = c

	return s[0], interfaceSlice, nil
}

func aggUnMapper(key string, data []interface{}) string {
	delimiter := "; "
	fieldsCount := 4

	var fields []string = make([]string, fieldsCount)
	fields[0] = key
	for k, d := range data {
		fields[k+1] = strconv.FormatFloat(math.Round(d.(float64)*100)/100, 'f', -1, 64)
		//fmt.Sprintf("%f", d)
	}

	return strings.Join(fields[:], delimiter)
}

func aggReducer(a, b []interface{}) ([]interface{}) {
	for i, _ := range a {
		sum:= a[i].(float64) + b[i].(float64)
		a[i] = sum
	}
	return a
}