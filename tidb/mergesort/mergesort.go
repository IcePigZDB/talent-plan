// Code is copy and learn from Jian Yang,look and rewrite.
package main

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
// TODO code here can not suit differern NCpu
func MergeSort(src []int64) {
	NCpu := 4
	s := mergeSorter(src, NCpu)
	copy(src, s)
}

// mergeSorter multi
func mergeSorter(src []int64, chunkCount int) []int64 {
	sliceSize := len(src)
	// fast path
	if sliceSize < chunkCount*1024 {
		return merger(src)
	}

	sliceParts := make([][]int64, chunkCount) // save chunCount chunk Results
	ch := make(chan []int64, chunkCount)      // chunk channel
	chunkSize := sliceSize / chunkCount       // chunk size

	// distribute sort chunk works to go routine
	for i := 0; i < chunkCount; i++ {
		// eg : 9%4=1,last chunk(3) has more than chunkSize(2)
		if sliceSize%chunkCount != 0 && chunkCount == i+1 {
			go sortSlice(src[i*chunkSize:sliceSize], ch)
		} else {
			go sortSlice(src[i*chunkSize:(i+1)*chunkSize], ch)
		}
	}
	// wait till go routines make things done.
	for i := 0; i < chunkCount; i++ {
		sliceParts[i] = <-ch
	}

	// return mergeCore(mergeCore(sliceParts[0], sliceParts[1]), mergeCore(sliceParts[2], sliceParts[3]))

	// faster then above after test.
	go mergeCoreWithCh(sliceParts[0], sliceParts[1], ch)
	go mergeCoreWithCh(sliceParts[2], sliceParts[3], ch)

	for i := 0; i < 2; i++ {
		sliceParts[i] = <-ch
	}
	return mergeCore(sliceParts[0], sliceParts[1])

}

// sortSlice : merge sort src slice
func sortSlice(src []int64, ch chan []int64) {
	ch <- merger(src)
}

// mergeCoreWithCh : return result to ch
func mergeCoreWithCh(a, b []int64, ch chan []int64) {
	ch <- mergeCore(a, b)
}

// mergeCore : two ordered slices merge to generate a new slice
func mergeCore(a, b []int64) []int64 {
	size, i, j := len(a)+len(b), 0, 0
	c := make([]int64, size, size)

	for k := 0; k < size; k++ {
		// if all of a are added to c,add b directly.
		if i >= len(a) {
			c[k] = b[j]
			j++
			continue
		}
		// i<len(a)&&b<len(b)&& b[j] <= a[i]
		if j < len(b) && b[j] <= a[i] {
			c[k] = b[j]
			j++
		} else
		// j>= len(b) or i<len(a)&&b<len(b)&& a[i] <= b[j]
		{
			c[k] = a[i]
			i++
		}
	}
	return c
}

// merger : single thread merge sort
func merger(s []int64) []int64 {
	if len(s) <= 1 {
		return s
	}
	m := len(s) / 2
	return mergeCore(merger(s[:m]), merger(s[m:]))
}
