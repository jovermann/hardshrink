#!/usr/bin/python
#
# sortarray.py - sort array.array arrays containing elements which cover multiple array indices
#
# Copyright (C) 2019 by Johannes Overmann <Johannes.Overmann@joov.de>

import random
import array
import time
import sys

def sortArray(data, elementSize):
    """Sort array.array data, assuming each element is 'elementSize' long.
    """
    if len(data) % elementSize:
        raise RuntimeError("array is not a multiple of elementSize")
    numElements = len(data) // elementSize
    if numElements == 0:
        return
    _sortArrayRange(data, elementSize, 0, numElements - 1)

    
def isSortedArray(data, elementSize):
    """Return True iff the array is empty or sorted.
    """
    numElements = len(data) // elementSize
    for i in range(0, numElements - 1):
        if _sortArrayLessThan(data, elementSize, i + 1, i):
            return False
    return True


def _sortArrayRange(data, elementSize, start, end):
    """Sort subrange of an array.array [start:end].
    
    Each element consists of 'elementSize' array indices.
    """
    if start >= end:
        return
    
    if (end - start) < 1000000:
        return _sortArrayRangeFast(data, elementSize, start, end)        
    
    s, e = start, end
    pivot = random.randint(start, end)
    
    # Create two partitions.
    while s <= e:
        # Skip elements which are already in the correct partition.
        while _sortArrayLessThan(data, elementSize, s, pivot): 
            s += 1
        while _sortArrayLessThan(data, elementSize, pivot, e):
            e -= 1
        if s <= e:
            _sortArraySwapElements(data, elementSize, s, e)
            if pivot == s:
                pivot = e
            elif pivot == e:
                pivot = s
            s += 1
            e -= 1
#        _dump(data, elementSize, "Step (start={}, end={}, s={}, e={}, pivot={})".format(start, end, s, e, pivot))
#    _dump(data, elementSize, "After partitioning (start={}, end={}, pivot={})".format(start, end, pivot))

    # Sort smaller partition first to limit max recursion depth to log(n).
    if (e - start) < (end - s):
        _sortArrayRange(data, elementSize, start, e)
        _sortArrayRange(data, elementSize, s, end)
    else:
        _sortArrayRange(data, elementSize, s, end)
        _sortArrayRange(data, elementSize, start, e)
        
        
def _sortArrayRangeFast(data, elementSize, start, end):
    """
    """
    l = list(range(start, end + 1))
    l.sort(key = lambda i: list(data[i * elementSize : (i + 1) * elementSize]))
    a = array.array("I")
    for i in l:
        a.extend(data[i * elementSize : (i + 1) * elementSize])
    data[start * elementSize : (end + 1) * elementSize] = a
    
    
def _sortArrayLessThan(data, elementSize, a, b):
    """Return True iff element a is less than element b.
    
    Each element consists of 'elementSize' array indices.
    """
    # Pull out index 0 just for speed.
    if data[a * elementSize] < data[b * elementSize]:
        return True
    if data[a * elementSize] > data[b * elementSize]:
        return False
    for i in range(1, elementSize):
        if data[a * elementSize + i] < data[b * elementSize + i]:
            return True
        if data[a * elementSize + i] > data[b * elementSize + i]:
            return False
    # Here: a == b
    return False
    
        
def _sortArraySwapElements(data, elementSize, a, b):
    """Swap elements a and b in array.array.

    Each element consists of 'elementSize' array indices.
    """
    # data[a], data[b] = data[b], data[a]
    tmp = data[a * elementSize : (a + 1) * elementSize]
    data[a * elementSize : (a + 1) * elementSize] = data[b * elementSize : (b + 1) * elementSize]
    data[b * elementSize : (b + 1) * elementSize] = tmp

    
def _dump(data, elementSize, title):
    """Print array.
    """
    numElements = len(data) // elementSize
    if title:
        print(title)
    for i in range(numElements):
        for j in range(elementSize):
            sys.stdout.write("{:08x} ".format(data[i * elementSize + j]))
        sys.stdout.write("\n")


def _test(size, elementSize):
    """
    """
    a = array.array("I")
    for i in range(size * elementSize):
        a.append(random.randint(0, 4294967295))
    if isSortedArray(a, elementSize):
        raise RuntimeError("isSorted() does not work")
#    _dump(a, elementSize, "Before sort:")
    start = time.time()
    sortArray(a, elementSize)
#    _sortArrayRangeFast(a, elementSize, 0, size - 1)
    elapsed = time.time() - start
#    _dump(a, elementSize, "After sort:")
    print("{:.1f}s ({:.1f}ns/element)".format(elapsed, elapsed / size * 1000000000.0))
    if not isSortedArray(a, elementSize):
        raise RuntimeError("sortArray() does not work")
   
    
    
    
    
def main():
    """Test sorting.
    """
    _test(100000, 10)
    
    
      
# call main()
if __name__ == "__main__":
    main()

