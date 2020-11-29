package main

import (
	"runtime"
	"strconv"
	"sync"
)

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {

	var wg sync.WaitGroup
	tbl0 := make([][]string, 0, 0)
	tbl1 := make([][]string, 0, 0)
	// read csvfile
	wg.Add(2)
	go func() {
		tbl0 = readCSVFileIntoTbl(f0)
		wg.Done()
	}()
	go func() {
		tbl1 = readCSVFileIntoTbl(f1)
		wg.Done()
	}()
	wg.Wait()

	// fast path iff len(tbl0),len(tbl1) < 1024(without test)
	if len(tbl0) < 1024 && len(tbl1) < 1024 {
		JoinExample(f0, f1, offset0, offset1)
	}

	// statusChan := make(chan bool, NCpu)
	// smaller one build. if tbl0 < tbl1 exchange,tbl0 build.
	// hashtable cost is huge
	// because smaller one is used to build hash table,there is less need to multi hashtable build.
	// left one is called inner table,right one is called outer table
	NCpu := runtime.NumCPU()
	if len(tbl0) < len(tbl1) {
		hashtbl := myBuildHashTable(tbl0, offset0, false)
		sum = myMultiProbe(hashtbl, tbl1, offset1, false, NCpu)
	} else {
		hashtbl := myBuildHashTable(tbl1, offset1, true)
		sum = myMultiProbe(hashtbl, tbl0, offset0, true, NCpu)
	}
	return sum
}

// myBuildHashTable build hashtable in one go routein in two sisuations:
// needExc add 1 to hashtbl;no need EXc add relation col0 value to hashtabl.
func myBuildHashTable(tbl [][]string, offset []int, needExc bool) *hashtbl {
	hashtbl := NewHashTable(len(tbl) / 2)
	var keyBuffer []byte
	for _, row := range tbl {
		for j, off := range offset {
			if j > 0 {
				keyBuffer = append(keyBuffer, '_')
			}
			keyBuffer = append(keyBuffer, []byte(row[off])...)
		}
		// needExc save 1 otherwise save relation0 col0's value
		if needExc {
			hashtbl.Put(keyBuffer, uint64(1))
		} else {
			value, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				panic("myBuildHashTable err :" + err.Error())
			}
			hashtbl.Put(keyBuffer, value)
		}
		// needExc:add 1 to
		keyBuffer = keyBuffer[:0]
	}
	return hashtbl
}

// myMultiProbe do myProbe in NumCPU go routines
func myMultiProbe(hashtbl *hashtbl, tbl [][]string, offset []int, needExc bool, chunkCount int) (sum uint64) {
	len := len(tbl)
	chunkSize := len / chunkCount
	sumCh := make(chan uint64, chunkCount)
	for i := 0; i < chunkCount; i++ {
		if len%chunkCount != 0 && chunkCount == i+1 {
			go myProbe(hashtbl, tbl[i*chunkSize:], offset, needExc, sumCh)
		} else {
			go myProbe(hashtbl, tbl[i*chunkSize:(i+1)*chunkSize], offset, needExc, sumCh)
		}
	}

	// wait for worker to return sum
	for i := 0; i < chunkCount; i++ {
		sum += <-sumCh
	}
	return sum
}

// myProbe do probe in two sisuations:
// needExc :sum+ = hashtbl.Get(key) * strconv.ParseUint(row[0],10,64)
// no needExc :sum +=hashtbl.Get(key)
func myProbe(hashtbl *hashtbl, chunk [][]string, offset []int, needExc bool, ch chan uint64) {
	var keyBuffer []byte
	var sum uint64
	for _, row := range chunk {
		for j, off := range offset {
			if j > 0 {
				keyBuffer = append(keyBuffer, '_')
			}
			keyBuffer = append(keyBuffer, []byte(row[off])...)
		}
		if needExc {
			num := hashtbl.Get(keyBuffer)
			// num = 0,reduce parse op.
			if num > 0 {
				value, err := strconv.ParseUint(row[0], 10, 64)
				if err != nil {
					panic("myProbe error : " + err.Error())
				}
				// cal sum
				sum += num * value
			}
		} else {
			sum += hashtbl.Get(keyBuffer)
		}
		// remember to reset
		keyBuffer = keyBuffer[:0]
	}
	ch <- sum
}
