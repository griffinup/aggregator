package aggregator

import (
	"errors"
	"github.com/spf13/afero"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

var agg AggContainer

func TestMain(m *testing.M) {
	agg = AggContainer{
		PathToFiles: "test/source",
		HeaderRow: "date; A; B; C",
		Fs:          afero.NewMemMapFs(),
	}

	agg.SetMapper(aggMapper)
	agg.SetUnMapper(aggUnMapper)
	agg.SetReducer(aggReducer)

	agg.Init()

	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestAggContainer_AddEntryToFile(t *testing.T) {
	key, data, _ := agg.Mapper("2019-10-10; 1; 2; 0.5")
	agg.addEntryToFile(key, data)

	_, err := agg.Fs.Stat(agg.PathToFiles + "/temp/2019-10-10.tmp")
	if os.IsNotExist(err) {
		t.Errorf("file \"%s\" does not exist.\n", agg.PathToFiles + "/temp/2019-10-10.tmp")
	}

	key, data, _ = agg.Mapper("2019-10-10; 4; -1; 1.25")
	agg.addEntryToFile(key, data)


	if b, _ := afero.FileContainsBytes(agg.Fs, agg.PathToFiles + "/temp/2019-10-10.tmp", []byte("2019-10-10; 5; 1; 1.75\n")); b != true {
		t.Errorf("file's \"%s\" content doesn't match.\n", agg.PathToFiles + "/2019-10-10.tmp")
	}

	agg.Fs.Remove(agg.PathToFiles + "/temp/2019-10-10.tmp")
}

func TestAggContainer_Start(t *testing.T) {
	afero.WriteFile(agg.Fs, agg.PathToFiles + "/source.cvs", []byte("date; A; B; C\n2018-03-01; 3; 4; 5.05\n2018-03-01; 1; 2; 1\n2018-03-01; 2; 2; -0.05\n2018-03-02; 5; 7; 6.06\n2018-03-03; 1; 2; 1.06"), 0644)
	agg.Start()
	if b, _ := afero.FileContainsBytes(agg.Fs, agg.ResultFile, []byte("date; A; B; C\n2018-03-01; 6; 8; 6\n2018-03-02; 5; 7; 6.06\n2018-03-03; 1; 2; 1.06")); b != true {
		t.Errorf("Result of aggregastion doesn't match.")
	}
}

func aggMapper(row string) (string, []interface{}, error) {
	fieldsCount := 4
	delimiter := ";"

	s := strings.Split(row, delimiter)
	if len(s) != fieldsCount {
		return "", nil, errors.New("Fields count mismatch")
	}

	var interfaceSlice = make([]interface{}, fieldsCount-1)
	a, err := strconv.ParseFloat(strings.TrimSpace(s[1]), 64)
	if err != nil {
		return "", nil, err
	}
	interfaceSlice[0] = a
	b, err := strconv.ParseFloat(strings.TrimSpace(s[2]), 64)
	if err != nil {
		return "", nil, err
	}
	interfaceSlice[1] = b
	c, err := strconv.ParseFloat(strings.TrimSpace(s[3]), 64)
	if err != nil {
		return "", nil, err
	}
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