
### 2026-04-19 00:19 UTC

```
goos: darwin
goarch: arm64
pkg: github.com/RichardKnop/minisql/benchmarks
cpu: Apple M1 Max
BenchmarkDelete_ByPK/minisql-10 	     123	   9626482 ns/op	  225775 B/op	     728 allocs/op
BenchmarkDelete_ByPK/sqlite-10  	   12877	     83772 ns/op	     447 B/op	      19 allocs/op
BenchmarkInsert_SingleRow/minisql-10         	     259	   4612691 ns/op	   72747 B/op	     182 allocs/op
BenchmarkInsert_SingleRow/sqlite-10          	   24850	     46801 ns/op	     311 B/op	      12 allocs/op
BenchmarkInsert_Batch/minisql-10             	     217	   6160346 ns/op	       100.0 rows/op	  892478 B/op	    6060 allocs/op
BenchmarkInsert_Batch/sqlite-10              	    4557	    242568 ns/op	       100.0 rows/op	   31792 B/op	    1300 allocs/op
BenchmarkSelect_PointScan/minisql-10         	   67018	     15314 ns/op	   23269 B/op	      87 allocs/op
BenchmarkSelect_PointScan/sqlite-10          	  329120	      3542 ns/op	     679 B/op	      26 allocs/op
BenchmarkSelect_FullScan/minisql-10          	     153	   7719704 ns/op	     10000 rows/op	12029816 B/op	  191727 allocs/op
BenchmarkSelect_FullScan/sqlite-10           	     222	   5357069 ns/op	     10000 rows/op	 1357747 B/op	   99758 allocs/op
BenchmarkSelect_CountStar/minisql-10         	     235	   4959024 ns/op	 3600899 B/op	   30508 allocs/op
BenchmarkSelect_CountStar/sqlite-10          	  119263	     10184 ns/op	     400 B/op	      13 allocs/op
BenchmarkSelect_RangeScan/minisql-10         	     193	   6046209 ns/op	 4782208 B/op	   68623 allocs/op
BenchmarkSelect_RangeScan/sqlite-10          	    1281	    910860 ns/op	   87997 B/op	    6581 allocs/op
BenchmarkTxn_NInserts/minisql-10             	     246	   5083184 ns/op	        50.00 rows/op	  477836 B/op	    3091 allocs/op
BenchmarkTxn_NInserts/sqlite-10              	    8146	    149147 ns/op	        50.00 rows/op	   16219 B/op	     658 allocs/op
BenchmarkUpdate_ByPK/minisql-10              	     273	   4294083 ns/op	   36777 B/op	      89 allocs/op
BenchmarkUpdate_ByPK/sqlite-10               	   29719	     39351 ns/op	     263 B/op	      10 allocs/op
PASS
ok  	github.com/RichardKnop/minisql/benchmarks	29.686s
1 ns/op	     263 B/op	      10 allocs/op
PASS
ok  	github.com/RichardKnop/minisql/benchmarks	29.686s
```

### 2026-04-19 00:20 UTC

```
goos: darwin
goarch: arm64
pkg: github.com/RichardKnop/minisql/benchmarks
cpu: Apple M1 Max
BenchmarkDelete_ByPK/minisql-10 	     133	   9334155 ns/op	  224037 B/op	     728 allocs/op
BenchmarkDelete_ByPK/sqlite-10  	   13101	     81853 ns/op	     447 B/op	      19 allocs/op
BenchmarkInsert_SingleRow/minisql-10         	     267	   4482520 ns/op	   73559 B/op	     184 allocs/op
BenchmarkInsert_SingleRow/sqlite-10          	   23706	     50192 ns/op	     311 B/op	      12 allocs/op
BenchmarkInsert_Batch/minisql-10             	     219	   7829629 ns/op	       100.0 rows/op	  892052 B/op	    6061 allocs/op
BenchmarkInsert_Batch/sqlite-10              	    4449	    240679 ns/op	       100.0 rows/op	   31806 B/op	    1300 allocs/op
BenchmarkSelect_PointScan/minisql-10         	   69458	     15839 ns/op	   23266 B/op	      87 allocs/op
BenchmarkSelect_PointScan/sqlite-10          	  337519	      3499 ns/op	     679 B/op	      26 allocs/op
BenchmarkSelect_FullScan/minisql-10          	     157	   7639268 ns/op	     10000 rows/op	12031262 B/op	  191727 allocs/op
BenchmarkSelect_FullScan/sqlite-10           	     222	   5368927 ns/op	     10000 rows/op	 1357748 B/op	   99758 allocs/op
BenchmarkSelect_CountStar/minisql-10         	     238	   4915726 ns/op	 3601522 B/op	   30508 allocs/op
BenchmarkSelect_CountStar/sqlite-10          	  117816	     10134 ns/op	     400 B/op	      13 allocs/op
BenchmarkSelect_RangeScan/minisql-10         	     193	   6043878 ns/op	 4782237 B/op	   68623 allocs/op
BenchmarkSelect_RangeScan/sqlite-10          	    1288	    914007 ns/op	   87997 B/op	    6581 allocs/op
BenchmarkTxn_NInserts/minisql-10             	     241	   5283940 ns/op	        50.00 rows/op	  478586 B/op	    3090 allocs/op
BenchmarkTxn_NInserts/sqlite-10              	    8136	    152366 ns/op	        50.00 rows/op	   16217 B/op	     658 allocs/op
BenchmarkUpdate_ByPK/minisql-10              	     291	   4591783 ns/op	   36375 B/op	      89 allocs/op
BenchmarkUpdate_ByPK/sqlite-10               	   28912	     39080 ns/op	     263 B/op	      10 allocs/op
PASS
ok  	github.com/RichardKnop/minisql/benchmarks	30.641s
0 ns/op	     263 B/op	      10 allocs/op
PASS
ok  	github.com/RichardKnop/minisql/benchmarks	30.641s
```
