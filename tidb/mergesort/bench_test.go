package main

import (
	"sort"
	"testing"
)

func BenchmarkMergeSort(b *testing.B) {
	// 2^24 = 16*1024*1024 ~= 16 0000 0000
	numElements := 16 << 20
	src := make([]int64, numElements)
	original := make([]int64, numElements)
	// prepare rand 2^24 number
	prepare(original)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// do not measure copy's time cose
		b.StopTimer()
		copy(src, original)
		b.StartTimer()
		MergeSort(src)
	}
}

func BenchmarkNormalSort(b *testing.B) {
	numElements := 16 << 20
	src := make([]int64, numElements)
	original := make([]int64, numElements)
	prepare(original)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		copy(src, original)
		b.StartTimer()
		// after test this sort with src[i]<src[j] return an array increasing.
		sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
	}
}
