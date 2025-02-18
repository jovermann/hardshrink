#!/usr/bin/python
#
# CompactStringPool.py - store millions strings in a memory efficient way
#
#
# Copyright (C) 2019 by Johannes Overmann <Johannes.Overmann@joov.de>

import array
import time
import random
import vsize


class CompactStringPool:
    """Pool of strings which are stored compactly.
    
    Strings can only ever be added to the pool, never removed.
    
    Strings are identified by opaque integer ids.
    
    The maximum supported string length is 2**64-1.
    """
    def __init__(self):
        """Constructor.
        """
        # Array of bytes storing all strings and their length information.
        # The length information is stored in the leading 1, 2 or 4 bytes,
        # depending on the length of the string. The id of the string indicates
        # the length of the length field in the lowest 2 bits.
        #
        # The opaque string ids have the following format:
        # bits[1..0]: Size of string length: 2**n bytes (1, 2, 4, or 8)
        # bits[..2]: Offset into self.strData.        
        self.str_data = array.array("B")

        
    def add(self, s):
        """Add string to pool and return opaque id for it.
        """
        l = len(s)
        id_ = len(self.str_data) << 2
        self.str_data.append(l & 255)
        if l >= 256:
            id_ += 1
            self.str_data.append((l >> 8) & 255)
            if l >= 65536:
                id_ += 1
                self.str_data.append((l >> 16) & 255)
                self.str_data.append((l >> 24) & 255)
                if l >= 2**32:
                    id_ += 1
                    self.str_data.append((l >> 32) & 255)
                    self.str_data.append((l >> 40) & 255)
                    self.str_data.append((l >> 48) & 255)
                    self.str_data.append((l >> 56) & 255)
        self.str_data.fromstring(s)
        return id_
            

    def get(self, id_):
        """Return string for id.
        """
        lenSize = id_ & 3
        offset = id_ >> 2
        l = self.str_data[offset]
        offset += 1
        if lenSize >= 1:
            l += self.str_data[offset] << 8
            offset += 1
        if lenSize >= 2:
            l += (self.str_data[offset] << 16) + (self.str_data[offset + 1] << 24)
            offset += 2
        if lenSize >= 3:
            l += (self.str_data[offset] << 32) + (self.str_data[offset + 1] << 40) + (self.str_data[offset + 2] << 48) + (self.str_data[offset + 3] << 56)
            offset += 4
        return self.str_data[offset: offset + l].tostring().decode()


class NonCompactStringPool:
    """Pool of strings, stored in a normal list.
    
    This has the same interface a CompactStringPool and can be used as a drop-in replacement.
    
    This mainly exists only to have a baseline for benchmarks, e.g. to compare the
    memory usage of CompactStringPool against this baseline.
    """
    def __init__(self):
        """Constructor.
        """
        self.str_data = []

        
    def add(self, s):
        """Add string to pool and return opaque id for it.
        """
        id_ = len(self.str_data)
        self.str_data.append(s)
        return id_
            

    def get(self, id_):
        """Return string for id.
        """
        return self.str_data[id_]

    
def self_test():
    """Run self-tests of this module.
    """
    print("Length encodig test ...")
    pool = CompactStringPool()
    id_list = array.array("L")
    n = 70000
    for i in range(n):
        id_list.append(pool.add("a" * i))
    print("Check ...")
    for i in range(n):
        s = pool.get(id_list[i])
        if len(s) != i:
            raise RunTimeError("Length encoding test failed")
#        if s != "a" * i: 
#            raise RunTimeError("String invariant test failed")
    print("OK")


def benchmark(PoolClass, n):
    """Run benchmarks.
    """
    random.seed(42)
    aaa = "a" * 1000
    start_time = time.time()
    pool = PoolClass()
    id_list = array.array("L")    
    string_bytes = 0
    start_vsize = vsize.get_vsize()
    for i in range(n):
        size = int(random.uniform(1, 20))
        size = 1000
        s = "{}".format(i) + "a" * size
        id_list.append(pool.add(s))
        string_bytes += len(s)
        if i == 0:
            start_vsize = vsize.get_vsize()            
    elapsed_time = time.time() - start_time
    mem_usage = vsize.get_vsize() - start_vsize
    print("{} strings in {}: {:.1f}MB ({:.1f}MB strings) {:.1f}s ({:.2f} overhead bytes per string)".format(n, PoolClass.__name__, mem_usage / 1024.0 / 1024.0, string_bytes / 1024.0 / 1024.0, elapsed_time, (mem_usage - string_bytes) / float(n)))
        
    
if __name__ == "__main__":
#    self_test()
    n = 1000*1000*2
    benchmark(CompactStringPool, n)
#    benchmark(NonCompactStringPool, n)

