# aggregator
    
Import:
```go
    import "github.com/griffinup/aggregator"
```
Create aggregator container:
```go
    agg := aggregator.AggContainer{
    		PathToFiles: os.Args[1],
    		HeaderRow:   "date; A; B; C",
    		FileExt:     "csv",
    }
```
Set your mapper and reducer functions:
```go
    agg.SetMapper(aggMapper)
    agg.SetUnMapper(aggUnMapper)
    agg.SetReducer(aggReducer)
```
Call Init() and Start()
```go
    agg.Init()
    agg.Start()
```
`agg.ResultFile` will contain path to file with aggregation results

Example of usage at /demo/sumbydate.go

    ./sumbydate /path/to/sourcefiles
