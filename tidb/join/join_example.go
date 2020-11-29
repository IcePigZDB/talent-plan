package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"join/mvmap"
	"os"
	"strconv"
	"unsafe"
	// "github.com/pingcap/tidb/util/mvmap"
)

// JoinExample performs a simple hash join algorithm.
// tbl0 build tbl1 probe
func JoinExample(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl(f0), readCSVFileIntoTbl(f1)
	hashtable := buildHashTable(tbl0, offset0)
	for _, row := range tbl1 {
		rowIDs := probe(hashtable, row, offset1)
		for _, id := range rowIDs {
			// ParseUnit parse str to uint base=10,bitsize 64
			v, err := strconv.ParseUint(tbl0[id][0], 10, 64)
			if err != nil {
				panic("JoinExample panic\n" + err.Error())
			}
			// cal sum
			sum += v
		}
	}
	return sum
}

// readCSVFileIntoTbl read csv file into tbl
func readCSVFileIntoTbl(f string) (tbl [][]string) {
	csvFile, err := os.Open(f)
	if err != nil {
		panic("ReadFileIntoTbl " + f + " fail\n" + err.Error())
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("ReadFileIntoTbl " + f + " fail\n" + err.Error())
		}
		tbl = append(tbl, row)
	}
	return tbl
}

func buildHashTable(data [][]string, offset []int) (hashtable *mvmap.MVMap) {
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	hashtable = mvmap.NewMVMap()
	for i, row := range data {
		for j, off := range offset {
			if j > 0 {
				keyBuffer = append(keyBuffer, '_')
			}
			keyBuffer = append(keyBuffer, []byte(row[off])...)
		}
		// value:rowID
		*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(i)
		hashtable.Put(keyBuffer, valBuffer)
		// empty keyBuffer
		keyBuffer = keyBuffer[:0]
	}
	return
}

func probe(hashtable *mvmap.MVMap, row []string, offset []int) (rowIDs []int64) {
	var keyHash []byte
	var vals [][]byte
	for i, off := range offset {
		if i > 0 {
			keyHash = append(keyHash, '_')
		}
		keyHash = append(keyHash, []byte(row[off])...)
	}
	vals = hashtable.Get(keyHash, vals)
	for _, val := range vals {
		rowIDs = append(rowIDs, *(*int64)(unsafe.Pointer(&val[0])))
	}
	return rowIDs
}

// gen correct anser here
func main() {
	// bigger table
	fmt.Printf("%X \n", JoinExample("./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{1}))       // 767636031
	fmt.Printf("%X \n", JoinExample("./t/r0.tbl", "./t/r1.tbl", []int{0}, []int{0}))       // 49082128576
	fmt.Printf("%X \n", JoinExample("./t/r0.tbl", "./t/r1.tbl", []int{1}, []int{1}))       // 85306117839070
	fmt.Printf("%X \n", JoinExample("./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{0}))       // 48860100254
	fmt.Printf("%X \n", JoinExample("./t/r0.tbl", "./t/r1.tbl", []int{0, 1}, []int{0, 1})) //5552101
	fmt.Printf("%X \n", JoinExample("./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{0}))       // 6331038719880
	fmt.Printf("%X \n", JoinExample("./t/r2.tbl", "./t/r2.tbl", []int{0, 1}, []int{0, 1})) // 42056985375886
}
