#!/usr/bin/python3
#
# hardshrink.py - find duplicate files and create hardlinks
#
# Requires Python >= 3.5 (for os.path.commonpath())
#
# Copyright (C) 2019-2020 by Johannes Overmann <Johannes.Overmann@joov.de>

import argparse
import hashlib
import os
import stat
import sys
import time
import itertools
import datetime
import struct
import sortarray
import array
import shutil

# For array and struct (Python >= 3.5):
# B = uint8_t
# H = uint16_t
# L = uint32_t
# Q = uint64_t

littleEndian = True

# Command line options.
options = None

# Print progress output once a second:
lastProgressTime = 0.0

def getTimeStr(seconds):
    """Get HH:MM:SS time string for seconds.
    """
    if seconds < 86400:
        return time.strftime('%H:%M:%S', time.gmtime(seconds))
    else:
        return time.strftime('{}d%H:%M:%S'.format(int(seconds) // 86400), time.gmtime(int(seconds)))


class Stat:
    """Global statistics.
    """
    def __init__(self):
        """Constructor.
        """
        self.numFilesRemoved = 0
        self.numBytesRemoved = 0
        self.numFilesLinked = 0
        self.numBytesLinked = 0
        self.numFilesHashed = 0
        self.numBytesHashed = 0
        self.numFilesHaveHash = 0
        self.numBytesHaveHash = 0
        self.numFilesRedundant = 0
        self.numBytesRedundant = 0
        self.numFilesTotal = 0
        self.numBytesTotal = 0
        self.numFilesSingletons = 0
        self.numBytesSingletons = 0
        self.numFilesNonSingletons = 0
        self.numBytesNonSingletons = 0
        self.numFilesUnique = 0
        self.numBytesUnique = 0
        self.numFilesSkipped = 0
        self.numBytesSkipped = 0
        self.currentDir = 0
        self.numDirsTotal = 0
        self.startTime = time.time()
        self.hashTime = 0
        self.numBytesInDb = 0

        # Used locally by findDuplicates() and callees.
        self.startTimeFindDuplicates = 0
        self.numFilesFindDuplicates = 0
        self.totalFilesFindDuplicates = 0
        self.sizeFindDuplicates = 0


    def printStats(self):
        """Print statistics.
        """
        self.numFilesDisk = self.numFilesUnique + self.numFilesRedundant
        self.numBytesDisk = self.numBytesUnique + self.numBytesRedundant

        self.elapsedTime = time.time() - self.startTime
        if self.elapsedTime == 0:
            self.elapsedTime = 0.0001
        if self.hashTime == 0:
            self.hashTime = 0.0001
        self.printAspect(self.numFilesTotal, self.numBytesTotal, "total, processed in {} ({}/s, {:.1f} files/s) (in {} dirs)".format(getTimeStr(self.elapsedTime), kB(self.numBytesTotal / self.elapsedTime), self.numFilesTotal / self.elapsedTime, self.numDirsTotal))
        self.printAspect(self.numFilesDisk, self.numBytesDisk, "disk usage total")
        self.printAspect(self.numFilesSingletons, self.numBytesSingletons, "singletons")
        self.printAspect(self.numFilesNonSingletons, self.numBytesNonSingletons, "non-singletons")
        self.printAspect(self.numFilesUnique, self.numBytesUnique, "unique files")
        self.printAspect(self.numFilesHaveHash, self.numBytesHaveHash, "have a hash")
        self.printAspect(self.numFilesRedundant, self.numBytesRedundant, "are redundant and not hardlinked")
        self.printAspect(self.numFilesHashed, self.numBytesHashed, "got a new hash (in {}, {}/s)".format(getTimeStr(self.hashTime), kB(self.numBytesHashed / self.hashTime)))
        self.printAspect(self.numFilesLinked, self.numBytesLinked, "got hardlinked")
        self.printAspect(self.numFilesRemoved, self.numBytesRemoved, "got removed")
        self.printAspect(self.numFilesSkipped, self.numBytesSkipped, "were skipped")
        numFilesTotal = self.numFilesTotal
        if numFilesTotal == 0:
            numFilesTotal = 1
        self.printAspect(self.numFilesTotal, self.numBytesInDb, "db entries ({:.1f} bytes/entry)".format(float(self.numBytesInDb) / numFilesTotal))


    def printAspect(self, numFiles, numBytes, aspectStr):
        """Print aspect.
        """
        numFilesTotal = self.numFilesTotal
        if numFilesTotal == 0:
            numFilesTotal = 1
        numBytesTotal = self.numBytesTotal
        if numBytesTotal == 0:
            numBytesTotal = 1
        print("{:8d} files ({:5.1f}%) and {} ({:5.1f}%) {}".format(numFiles, numFiles * 100.0 / numFilesTotal, kB(numBytes), numBytes * 100.0 / numBytesTotal, aspectStr))


stats = Stat()


def strToBytes(filename):
    """Return bytes for a str filename.
    """
    return filename.encode("utf8")


def bytesToStr(filename):
    """Return str for a bytes filename.
    """
    return filename.decode("utf8", errors="backslashreplace")


def formatFloat(f, width):
    """Format positive floating point number into width chars.

    width should be >= 3 and must be >= 1.
    """
    # This is surpisingly complex due to rounding.

    # First get the number of digit before the point (roughly, due to rounding).
    s = "{:.0f}".format(f)
    prec = width - len(s) - 1
    if prec < 0:
        return s

    # Then try with one more digit of precision than would normally fit.
    # This allows to handle 9.9 for width=3 graefully, and also 9.99 for width=4.
    s = "{:.{p}f}".format(f, p = prec + 1)
    if len(s) == width:
        return s

    # This is the common case.
    s = "{:{w}.{p}f}".format(f, w = width, p = prec)
    return s


def kB(n, width = 7):
    """Get a nicely formatted string for integer n with the suffixes B, kB, MB, GB, TB, PB, EB.

    width must be >= 5

    Examples:
       987B
    9.876KB
    98.76KB
    987.6KB
    9.876MB
    98.76MB
    987.6MB
    9.876GB
    98.76GB
    987.6GB
    9.876TB
    ...
    """
    n = int(n)
    if n < 0:
        raise RuntimeError("kb(x) for x < 0 called")
    if width < 5:
        raise RuntimeError("kb(x,w) for w < 5 called")
    if n < 1000:
        return "{:{w}d}".format(n, w = width - 1) + "B"
    if n < 1000 * 1024:
        return formatFloat(n / 1024.0, width - 2) + "kB"
    if n < 1000 * 1024 * 1024:
        return formatFloat(n / 1024.0 / 1024.0, width - 2) + "MB"
    if n < 1000 * 1024 * 1024 * 1024:
        return formatFloat(n / 1024.0 / 1024.0 / 1024.0, width - 2) + "GB"
    if n < 1000 * 1024 * 1024 * 1024 * 1024:
        return formatFloat(n / 1024.0 / 1024.0 / 1024.0 / 1024.0, width - 2) + "TB"
    if n < 1000 * 1024 * 1024 * 1024 * 1024 * 1024:
        return formatFloat(n / 1024.0 / 1024.0 / 1024.0 / 1024.0 / 1024.0, width - 2) + "PB"
    return formatFloat(n / 1024.0 / 1024.0 / 1024.0 / 1024.0 / 1024.0 / 1024.0, width - 2) + "EB"


def remainingTimeStr(startTime, current, total):
    """Get string showing the remaining time for an operation.

    From current to total, started on startTime.
    """
    elapsedTime = time.time() - startTime
    if current == 0:
        current = 1
    totalTime = (elapsedTime / current) * total
    remainingTime = totalTime - elapsedTime
    return getTimeStr(remainingTime)


def progressStr(startTime, current, total):
    """Get progress string (a/b, HH::MM:SS remaining).
    """
    return "{}/{} ({:.1f}% done, {} remaining)".format(current, total, current * 100.0 / total, remainingTimeStr(startTime, current, total))


def getNumHardlinks(path):
    """Get number of hardlinks.
    """
    statinfo = os.lstat(path)
    return statinfo.st_nlink


def write64(f, x):
    """Write 8 bytes to a file.

    x may be and int or a str.
    """
    if type(x) is int:
        if x < 0 or x >= 2**64:
            raise RuntimeError("write64(): argument out of range")
        f.write(struct.pack("<Q", x))
    elif type(x) is str:
        s = x.encode("latin1")
        if len(s) != 8:
            raise RuntimeError("write64(): string argument must be 8 bytes ({})".format(s))
        f.write(s)
    else:
        raise RuntimeError("write64(): unsupported type")


def read64(f):
    """Read 8 bytes from a file and return as an 64-bit unsigned int (little endian).
    """
    return struct.unpack("<Q", f.read(8))[0]


def read64str(f):
    """Read 8 bytes from a file and return as an ASCII string.
    """
    return f.read(8).decode("latin1")


def addString(array, b):
    """Add bytes to array.array("B") (uint8_t) in the format understood by readString() (prefixed by its length).
    """
    offset = len(array)
    if len(b) <= 0xfc:
        array.append(len(b))
    elif len(b) <= 0xffff:
        array.append(0xfd)
        array.frombytes(struct.pack("<H", len(b)))
    elif len(b) <= 0xffffFFFF:
        array.append(0xfe)
        array.frombytes(struct.pack("<L", len(b)))
    else:
        array.append(0xff)
        array.frombytes(struct.pack("<Q", len(b)))
    array.frombytes(b)
    return offset


def readString(array, offset):
    """Read bytes from array.array("B") (uint8_t) at offset.
    """
    l = array[offset]
    offset += 1
    if l == 0xfd:
        l = struct.unpack("<H", array[offset : offset + 2])[0]
        offset += 2
    elif l == 0xfe:
        l = struct.unpack("<L", array[offset : offset + 4])[0]
        offset += 4
    elif l == 0xff:
        l = struct.unpack("<Q", array[offset : offset + 8])[0]
        offset += 8
    return array[offset: offset + l].tobytes()


def calcHash(path):
    """Calculate hash for file.

    We use sha1 since this is the fastest algorithm in hashlib (except for
    md4). The current collision attacks are of no concern for the purpose
    of discriminating (just) 10**12 different files or less.
    """
    if options.verbose >= 2:
        print("Hashing {}".format(bytesToStr(path)))

    startTime = time.time()
    blockSize = 65536
    hash = hashlib.sha1()
    numBytes = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(blockSize), b""):
            hash.update(block)
            numBytes += len(block)
    stats.numBytesHashed += numBytes
    stats.numFilesHashed += 1
    stats.hashTime += time.time() - startTime
    return hash.digest()


def progressDue():
    """Return True iff printing progress information is due (once a second).
    """
    global lastProgressTime
    if not options.progress:
        return False
    t = time.time()
    if t - lastProgressTime >= 1.0:
        lastProgressTime = t
        return True
    return False


needLf = False


def printProgress(s):
    """Print progress string.
    """
    global needLf
    if not options.progress:
        return
    sys.stdout.write(" ")
    sys.stdout.write(s)
    sys.stdout.write("    \r")
    sys.stdout.flush()
    needLf = True


def printLf(s):
    """Print LF if progress is enabled and if the cursor is on a progress line.
    """
    global needLf
    if needLf:
        print()
        needLf = False
    print(s)


def print_nolf(s):
    """Print without trailing linefeed.
    """
    sys.stdout.write(s)
    sys.stdout.flush()


class Entry:
    def __init__(self, hardshrinkDb, index):
        """Constructor.
        """
        self.clear()
        self.hardshrinkDb = hardshrinkDb
        self.index = index


    def clear(self):
        """Clear entry.
        """
        self.mtime = 0
        self.inode = 0
        self.size = 0
        self.path = ""
        self.hash = array.array("Q", [0,0,0])


    def dump(self):
        """Print entry.
        """
        datestr = datetime.datetime.fromtimestamp(self.mtime / 1000000.0).replace(microsecond=0).isoformat()
        hash = self.hash[:]
        if littleEndian:
            hash.byteswap()
        print("{} {} {:10d} {} {}".format(hash.tobytes().hex(), datestr, self.inode, kB(int(self.size)), bytesToStr(self.path)))


    def setInodeMtime(self, inode, mtime):
        """Set inode and mtime and also modify the original valies in the associated hardshrinkDb.
        """
        self.inode = inode
        self.mtime = mtime
        self.hardshrinkDb.setInodeMtime(self.index, inode, mtime)


    def setHashAndMtime(self, hash, mtime):
        """Set hash and mtime.

        Hash must be an array.array("Q") with len=3.
        """
        self.hash = hash
        self.mtime = mtime
        self.hardshrinkDb.setHashAndMtime(self.index, self.hash, self.mtime)


    def updateInodeAndMtime(self):
        """Update inode and mtime from filesystem.

        Also update db accordingly.
        """
        statinfo = os.lstat(self.path)
        self.setInodeMtime(statinfo.st_ino, int(statinfo.st_mtime * 1000000))


    def hasHash(self):
        """Return True iff entry has a hash.
        """
        return (self.hash[0] != 0) or (self.hash[1] != 0) or (self.hash[2] != 0)


    def calcHash(self):
        """Update hash from file content.
        """
        self.hash = self.hardshrinkDb.calcHash(self.index)


    def clearHash(self):
        """Clear hash.
        """
        self.hash = array.array("Q", [0,0,0])
        self.hardshrinkDb.setHashAndMtime(self.index, self.hash, self.mtime)



class HardshrinkDb:
    def __init__(self):
        """Constructor.
        """
        self.clear()


    def clear(self):
        """Clear container.
        """
        self.data = array.array("Q")
        self.entrySize = 8
        self.stringData = array.array("B")
        self.rootDir = ""
        self.iterator = 0
        self.dirty = False
        self.dbFile = ""


    def resetIterator(self):
        """Reset iterator to the start of the list.
        """
        if options.reverse:
            self.iterator = 0
        else:
            self.iterator = self.getNumFiles() - 1


    def isIteratorValid(self):
        """Return True iff iterator is valid.
        """
        return (self.iterator >= 0) and (self.iterator < self.getNumFiles())


    def getCurrentItem(self, advance = False):
        """Get current item under iterator, optionally incrementing iterator.

        This returns an Entry.
        """
        r = self.getEntry(self.iterator)
        if advance:
            if options.reverse:
                self.iterator += 1
            else:
                self.iterator -= 1
        return r


    def getCurrentItemSize(self):
        """Get current sort key for item hash under iterator.

        We intentionally return the full entry, not just the hash, because this
        key is used to sort HardshrinkDB containers during the merge-sort, and
        these must be sorted in the same way as sortarray.sortArray does, and
        this also uses the full entry.
        """
        if self.isIteratorValid():
            offset = self.iterator * self.entrySize
            return self.data[offset]
        else:
            # Return the highest size value to make sure that DBs which ran
            # out of items are sorted to the end by getNextFileListWithTheSameSize()
            # so they can get removed from the DB list.
            if options.reverse:
                return 0xffffFFFFffffFFFF
            else:
                return 0


    def load(self, filename):
        """Load hardshrink db from file.

        Throw an error if file does not exist or has the wrong format.

        File format of the .hardshrinkdb file:

        uint64_t magic "HRDSHRNK"
        uint64_t chunk_name "HEADER  "
        uint64_t chunk_size (in bytes without chunk_name and chunk_size) (8)
        uint64_t version (0)
        uint64_t chunk_name "DATA    "
        uint64_t chunk_size (in bytes without chunk_name and chunk_size)
        uint64_t entry_size (in bytes) (64)
        uint64_t data[(chunk_size - 8) / 8]
        uint64_t chunk_name "STRDATA "
        uint64_t chunk_size (in bytes without chunk_name and chunk_size)
        uint8_t  stringData[chunk_size]

        Format of one entry in 'data': uint64_t data[8]:
        (Affects entrySize, getEntry(), scanDir(), setInodeMtime(), setHashAndMtime())
        uint64_t[0]: size
        uint64_t[1]: sha1[159..96]
        uint64_t[2]: sha1[95..32]
        uint64_t[3]: hi:sha1[31..0] lo:0
        uint64_t[4]: inode
        uint64_t[5]: mtime   # mtime in microseconds since 1970-01-01 00:00 (covers 584542 years)
        uint64_t[6]: dir_offset
        uint64_t[7]: filename_offset

        Format of stringData:
        <size><string>...
        Where <size> is:
        00..FC: 8-bit length
        FD XX XX: 16-bit length in little endian
        FE XX XX XX XX: 32-bit length in little endian
        FF XX XX XX XX XX XX XX XX: 64-bit length in little endian

        The entries in the file can not be assumed to be sorted.
        """
        if options.verbose >= 1:
            print_nolf("Reading db from {}\r".format(bytesToStr(filename)))
        self.clear()
        self.dbfile = filename
        self.rootDir = os.path.dirname(filename)
        with open(filename, "rb") as f:
            if read64str(f) != "HRDSHRNK":
                raise RuntimeError("load(): wrong magic")

            # Read header chunk.
            if read64str(f) != "HEADER  ":
                raise RuntimeError("load(): missing header chunk")
            chunkSize = read64(f)
            if chunkSize != 8:
                raise RuntimeError("load(): unsupported header length {}".format(chunkSize))
            version = read64(f)
            if version != 0:
                raise RuntimeError("load(): unsupported version")

            # Read data chunk.
            if read64str(f) != "DATA    ":
                raise RuntimeError("load(): missing data chunk")
            chunkSize = read64(f)
            entrySizeBytes = read64(f)
            if entrySizeBytes != self.entrySize * 8:
                raise RuntimeError("load(): unsupported entry size")
            self.data.fromfile(f, (chunkSize - 8) // 8)

            # Read stringData chunk.
            if read64str(f) != "STRDATA ":
                raise RuntimeError("load(): missing stringData chunk")
            chunkSize = read64(f)
            self.stringData.fromfile(f, chunkSize)

        self.sort()
        self.dirty = False

        if options.verbose >= 1:
            print("{}: {} files, {}, {:.1f} bytes/entry    ".format(os.path.dirname(bytesToStr(filename)), self.getNumFiles(), kB(self.getTotalDbSizeInBytes()), self.getTotalDbSizeInBytes() / self.getNumFiles()))


    def save(self, filename):
        """Save hardshrink db to file.
        """
        if options.verbose >= 1:
            print("Writing db to {} ({})".format(bytesToStr(filename), kB(self.getTotalDbSizeInBytes())))
        self.dbfile = filename
        with open(filename, "wb") as f:
            write64(f, "HRDSHRNK")

            # Write header chunk.
            write64(f, "HEADER  ")
            write64(f, 8) # chunk_size
            write64(f, 0) # version

            # Write data chunk.
            write64(f, "DATA    ")
            write64(f, len(self.data) * 8 + 8) # chunk_size
            write64(f, self.entrySize * 8)
            self.data.tofile(f)

            # Write stringData chunk.
            write64(f, "STRDATA ")
            write64(f, len(self.stringData)) # chunk_size
            self.stringData.tofile(f)

        self.dirty = False


    def scanDir(self, dir):
        """Scan directrory and populate db.
        """
        if options.verbose >= 1:
            print("Scanning dir {}".format(bytesToStr(dir)))
        self.clear()
        self.rootDir = dir
        totalSize = 0
        startTime = time.time()
        for root, dirs, files in os.walk(dir):
            if options.verbose >= 2:
                print("Dir {}".format(bytesToStr(root)))
            subdir = root[len(dir) + 1:]
            dirOffset = addString(self.stringData, subdir)
            for f in files:
                # Get meta-data.
                path = os.path.join(root, f)
                statinfo = os.lstat(path)
                # Only process regular files.
                if not stat.S_ISREG(statinfo.st_mode):
                    stats.numFilesSkipped += 1
                    continue
                # Ignore .hardshringdb files.
                if f == options.db_bytes:
                    continue
                size = statinfo.st_size
                fileOffset = addString(self.stringData, f)
                mtime = int(statinfo.st_mtime * 1000000)
                inode = statinfo.st_ino

                # Create and append entry without hash.
                self.data.append(size)
                self.data.append(0)
                self.data.append(0)
                self.data.append(0)
                self.data.append(inode)
                self.data.append(mtime)
                self.data.append(dirOffset)
                self.data.append(fileOffset)

                if options.verbose >= 3:
                    print("File {}".format(bytesToStr(path)))

                totalSize += size
                if progressDue():
                    printProgress("Scan {} {} {} {} {}".format(progressStr(stats.startTime, stats.currentDir, stats.numDirsTotalCmdLine), os.path.basename(bytesToStr(dir)), len(self.data) // self.entrySize, kB(totalSize), bytesToStr(path)[-options.progress_width:]))
        if options.verbose >= 1:
            elapsed = time.time() - startTime
            print("Scanned {} files ({}) in {} {}".format(len(self.data) // self.entrySize, kB(totalSize), getTimeStr(elapsed), " " * options.progress_width))

        self.sort()
        self.dirty = True


    def sort(self):
        """Sort entries to size, hash, inode and then mtime, in place.
        """
        sortarray.sortArray(self.data, self.entrySize)


    def getEntry(self, index):
        """Get nth entry in db.
        """
        i = index * self.entrySize
        e = Entry(self, index)
        e.size = self.data[i + 0]
        e.hash = self.data[i + 1 : i + 4]
        e.inode = self.data[i + 4]
        e.mtime = self.data[i + 5]
        dir = readString(self.stringData, self.data[i + 6])
        filename = readString(self.stringData, self.data[i + 7])
        e.path = os.path.join(self.rootDir, dir, filename)
        return e


    def calcHash(self, index):
        """Calc hash for an entry.

        This updates the hash in self.data and also returns it.
        """
        i = index * self.entrySize
        dir = readString(self.stringData, self.data[i + 6])
        filename = readString(self.stringData, self.data[i + 7])
        path = os.path.join(self.rootDir, dir, filename)
        hash = calcHash(path)
        if len(hash) != 20:
            raise RuntimeError("wrong hash len")
        hash += b"\0\0\0\0"
        data = array.array("Q")
        data.frombytes(hash)
        if littleEndian:
            data.byteswap()
        self.data[i + 1 : i + 4] = data
        self.dirty = True
        return data


    def setHashAndMtime(self, index, hash, mtime):
        """Set hash and mtime.

        Hash must be an array.array("Q") with len=3.
        """
        i = index * self.entrySize
        self.data[i + 1 : i + 4] = hash
        self.data[i + 5] = mtime
        self.dirty = True


    def getNumFiles(self):
        """Get number of files in this container.
        """
        return len(self.data) // self.entrySize


    def getNumBytesInFiles(self):
        """Get number of bytes in all files.
        """
        return sum((self.getEntry(i).size for i in range(0, self.getNumFiles())))


    def getTotalDbSizeInBytes(self):
        """Get total size of this container.

        This only accounts for the actual data, not any preallocated memory of the arrays.
        """
        headerSize = 8 + 8 + 8
        dataSize = 8 + 8 + 8 + len(self.data) * 8
        stringSize = 8 + 8 + len(self.stringData)
        return 8 + headerSize + dataSize + stringSize


    def dump(self):
        """Print db.
        """
        print("HardshrinkDB for dir \"{}\":".format(bytesToStr(self.rootDir)))
        bytesPerFile = self.getTotalDbSizeInBytes() / self.getNumFiles()
        print("({} files, {} total, {} data, {} strings, {:.1f} bytes/db entry)".format(self.getNumFiles(), kB(self.getTotalDbSizeInBytes()), kB(len(self.data) * 8), kB(len(self.stringData)), bytesPerFile))
        for i in range(0, self.getNumFiles()):
            self.getEntry(i).dump()


    def setInodeMtime(self, index, inode, mtime):
        """Set inode and mtime of entry at index.
        """
        i = index * self.entrySize
        self.data[i + 4] = inode
        self.data[i + 5] = mtime
        self.dirty = True



def getNextFileListWithTheSameSize(dbList):
    """Return next list of Entries which have the same size.

    The returned list is not sorted in any way.

    This works like an n-way mergesort step.

    dbList must not be empty, but the dbs in it may be empty.

    This may delete entries from dbList until it is empty.
    """
    dbList.sort(key = lambda x: x.getCurrentItemSize(), reverse = not options.reverse)
    if not dbList[0].isIteratorValid():
        del dbList[0]
        return []
    r = [dbList[0].getCurrentItem(advance = True)]
    for db in dbList:
        while db.isIteratorValid() and (db.getCurrentItemSize() == r[0].size):
            r.append(db.getCurrentItem(advance = True))

    # Remove last db in case we processed all entries.
    if not dbList[-1].isIteratorValid():
        del dbList[-1]

    return r


def getListOfFileListsWithIdenticalHashes(files, justPropagateExistingHashes):
    """Get list of file lists where each inner list is a list of files with identical hashes.
    """
    # If we have no files at all we return an empty list.
    if len(files) == 0:
        return []

    # If we have just one file (for this size) we do not need to calculate the hash.
    if len(files) == 1:
        return [files]

    # Create map from inode to entry with the most recent mtime.
    # Also clear all outdated or questionable hashes.
    # (Missing hashes will be recalulated (or copied) in the next step.)
    inodeToEntry = {}
    for entry in files:
        if entry.inode not in inodeToEntry:
            inodeToEntry[entry.inode] = entry
        else:
            if entry.mtime > inodeToEntry[entry.inode].mtime:
                # Entries with newer mtime always have priority. Potential hashes of old entries are ignored (and cleared) since they are most likely outdated.
                inodeToEntry[entry.inode].clearHash()
                inodeToEntry[entry.inode] = entry
            elif entry.mtime == inodeToEntry[entry.inode].mtime:
                # Entries with identical size and mtime:
                if entry.hasHash():
                    if inodeToEntry[entry.inode].hasHash():
                        if entry.hash != inodeToEntry[entry.inode].hash:
                            # Inconsistent hashes for the same inode, same size an the same mtime: This indicates trouble and is worth a warning.
                            # To be conservative we remove the hashes from both entries since we do not know which one to trust.
                            print("Warning: Inconsistent hashes for two files with the same inode, same size and same mtime: Will ignore and re-calculate hashes:")
                            entry.dump()
                            inodeToEntry[entry.inode].dump()
                            entry.clearHash()
                            inodeToEntry[entry.inode].clearHash()
                        else:
                            # Identical hashes for identical inodes and identical mtimes:
                            # We arbitrarily use the entry which is already in the map. It does not matter.
                            pass
                    else:
                        # Prefer entries which have a hash over those which do not have a hash.
                        inodeToEntry[entry.inode] = entry
                else:
                    # Entry does not have a hash yet. It does not matter whether the entry in the map already has a hash or not.
                    # We arbitrarily keep the entry which is already in the map.
                    pass
            else:
                # entry.mtime < inodeToEntry[entry.inode].mtime:
                # Ignore outdated entry and clear hash.
                entry.clearHash()

    # For --update do not calculate new hashes (yet). Just re-use existing hashes.
    # Copy hashes from entries having the same inode, size and mtime.
    if justPropagateExistingHashes:
        for entry in files:
            if not entry.hasHash():
                if inodeToEntry[entry.inode].hasHash():
                    entry.setHashAndMtime(inodeToEntry[entry.inode].hash, inodeToEntry[entry.inode].mtime)
            else:
                if entry.hash != inodeToEntry[entry.inode].hash:
                    raise RuntimeError("Internal error: Inconsistent hashes!")
        # Return None to make sure the result is not used (as a list), because the following code will generate invalid file lists (for example a list of all files which do not yet have a hash.).
        return None

    if len(inodeToEntry) > 1:
        # Calculate missing hashes for all inodes which do not yet have a hash.
        for (inode, entry) in inodeToEntry.items():
            if not entry.hasHash():
                entry.calcHash()

        # Update the hashes of all files according to the map.
        # Copy hashes from entries having the same inode, size and mtime.
        for entry in files:
            if not entry.hasHash():
                entry.setHashAndMtime(inodeToEntry[entry.inode].hash, inodeToEntry[entry.inode].mtime)
            else:
                if entry.hash != inodeToEntry[entry.inode].hash:
                    raise RuntimeError("Internal error: Inconsistent hashes for different files pointing to the same inode!")

    # Sort by hash, mtime and then inode
    files = sorted(files, key = lambda x: (x.hash, x.mtime, x.inode))

    # Split list into lists with the same hashes.
    currentList = []
    r = []
    for entry in files:
        if (len(currentList) > 0) and (entry.hash != currentList[0].hash):
            # Emit currentList.
            r.append(currentList)
            # Create new list.
            currentList = [entry]
        else:
            currentList.append(entry)
    # Emit last currentList.
    if len(currentList) > 0:
        r.append(currentList)

    return r


def printProgressProcessingFiles(numAdditionalFiles = 0, message = ""):
    """Print progress output while processing files.

    This is called from multiple locations to indicate progress.
    """
    printProgress("Processing file {} (size {}{})        ".format(progressStr(stats.startTimeFindDuplicates, stats.numFilesFindDuplicates + numAdditionalFiles, stats.totalFilesFindDuplicates), kB(stats.sizeFindDuplicates), message))


def findDuplicates(dbList_, func, justPropagateExistingHashes = False):
    """Find duplicate files.

    all DBs must be sorted.

    func() is called for each group of files with identical content, oldest file first.
    """
    if len(dbList_) == 0:
        return

    # Reset iterators.
    for db in dbList_:
        db.resetIterator()

    # Not a deepcopy. We just want to preserve the order of the original dbList since dbList is permuted and cleared in the following.
    dbList = dbList_.copy()
    stats.totalFilesFindDuplicates = sum([db.getNumFiles() for db in dbList])
    stats.numFilesFindDuplicates = 0
    stats.startTimeFindDuplicates = time.time()
    while len(dbList) > 0:
        # Get list of files with the same size.
        allFiles = getNextFileListWithTheSameSize(dbList)
        if len(allFiles) == 0:
            break

        # Check size.
        size = allFiles[0].size
        if not justPropagateExistingHashes:
            if size < options.min_size:
                continue
            if size > options.max_size:
                continue

        # Get list of lists where each inner lists contains files with identical content (ideentical hash).
        fileLists = getListOfFileListsWithIdenticalHashes(allFiles, justPropagateExistingHashes)
        if justPropagateExistingHashes:
            continue
        if len(fileLists) == 0:
            break
        stats.sizeFindDuplicates = size

        if options.verbose >= 1:
            numInodes = len(set((f.inode for f in allFiles)))
            printLf("Processing size {} ({}) ({} files, {} inodes, {} unique hashes)".format(size, kB(size, 5), len(allFiles), numInodes, len(fileLists)))

        for files in fileLists:
            # Process identical files.

            # Update stats.
            stats.numFilesUnique += 1
            stats.numBytesUnique += files[0].size
            if len(files) == 1:
                stats.numFilesSingletons += 1
                stats.numBytesSingletons += files[0].size
            else:
                stats.numFilesNonSingletons += sum((1 for f in files))
                stats.numBytesNonSingletons += sum((f.size for f in files))
                inodes = set([files[0].inode])
                for f in files[1:]:
                    if f.inode not in inodes:
                        stats.numFilesRedundant += 1
                        stats.numBytesRedundant += f.size
                        inodes.add(f.inode)
            stats.numFilesTotal += len(files)
            stats.numBytesTotal += sum((f.size for f in files))
            stats.numFilesHaveHash += sum((1 for f in files if f.hasHash()))
            stats.numBytesHaveHash += sum((f.size for f in files if f.hasHash()))

            # Call the actual processing function.
            func(files)

            stats.numFilesFindDuplicates += len(files)

            # Fallback id func does not print progress.
            if progressDue():
                printProgressProcessingFiles()


def printDuplicates(files):
    """Print all duplicate files.
    """
    if len(files) > 1:
        print("{} identical files:".format(len(files)))
        for f in files:
            f.dump()


def printSingletons(files):
    """Print all singleton files.
    """
    if len(files) == 1:
        for f in files:
            f.dump()


def printAll(files):
    """Print all duplicate files.
    """
    if len(files) == 1:
        print("Singleton:")
    else:
        print("{} identical files:".format(len(files)))
    for f in files:
        f.dump()


def processDir(dir):
    """Process directory.
    """
    dbfile = os.path.join(dir, strToBytes(options.db))
    db = HardshrinkDb()
    if (not options.force_scan) and os.path.isfile(dbfile):
        db.load(dbfile)
        if options.update:
            # Updating essentially means:
            # We want the new db to look _exactly_ like the current directory tree,
            # perhaps removing and adding files compared to the old db.
            # Thus we need to rescan the tree it completely.
            # But we want to avoid re-calculating the hashes for files which have
            # the same inode, size and mtime, so take these from the old db.
            # This supports moving and renaming files.
            dbnew = HardshrinkDb()
            dbnew.scanDir(dir)
            dbList = [db, dbnew]
            findDuplicates(dbList, None, justPropagateExistingHashes = True)
            db = dbnew
            db.save(dbfile)
    else:
        if options.ignore_dirs_without_db:
            return None
        # Fresh scan of directory. Ignore any existing db file.
        db.scanDir(dir)
        db.save(dbfile)
    if options.dump:
        db.dump()
        stats.numFilesTotal += db.getNumFiles()
        stats.numBytesTotal += db.getNumBytesInFiles()
    return db


def getTmpName(path):
    """Return non-existing filename.
    """
    for i in range(10000):
        tmpname = os.path.join(os.path.dirname(path), strToBytes(os.urandom(4 + i // 1000).hex()) + b".tmp")
        if not os.path.exists(tmpname):
            return tmpname
    raise RuntimeError("Error finding temp name")


def breakHardlink(f):
    """Break hardlink by creating a new copy of the file.
    """
    tmpname = getTmpName(f.path)
    shutil.copy2(f.path, tmpname)
    if os.name == "nt":
        os.unlink(f.path)
    os.rename(tmpname, f.path)
    f.updateInodeAndMtime()


def linkTwoFiles(a, b):
    """Link second file to first. Keep first. Replace second by hardlink to first.
    """
    # Sanity checks.
    if id(a) == id(b):
        raise RuntimeError("Internal error: linkTwoFiles() on the same file!")
    if (not a.hasHash()) or (not b.hasHash()):
        raise RuntimeError("Internal error: linkTwoFiles() files have not been hashed!")
    if a.hash != b.hash:
        raise RuntimeError("Internal error: linkTwoFiles() on files with different hashes!")
    if a.size != b.size:
        raise RuntimeError("Internal error: linkTwoFiles() on files with different size!")
    if a.inode == b.inode:
        raise RuntimeError("Internal error: linkTwoFiles() on files with the same inode!")
    # This always holds because files are sorted by ascending mtime.
    if a.mtime > b.mtime:
        raise RuntimeError("Internal error: linkTwoFiles(): First file must be older than second!")

    # Hardlink a to b.
    if options.verbose >= 2:
        fromfile = bytesToStr(a.path)
        tofile = bytesToStr(b.path)
        print("Link {} -> {}".format(fromfile, tofile))
    if not options.dummy:
        numHardlinks = getNumHardlinks(b.path)
        if numHardlinks == 1:
            stats.numBytesRemoved += b.size
            stats.numFilesRemoved += 1
        tmpname = getTmpName(b.path)
        os.link(a.path, tmpname)
        if os.name == "nt":
            # Not atomic under Windows.
            os.unlink(b.path)
        os.rename(tmpname, b.path) # Implicitly also does unlink(b.path).
        b.setInodeMtime(a.inode, a.mtime)
        stats.numBytesLinked += b.size
        stats.numFilesLinked += 1


def linkFiles(files):
    """Link files to first file (which is supposed to be the oldest inode).

    Honour the maximum number of hardlinks per files which is 65000 on
    Linux/ext[234] and about 1024 on Windows.
    """
    base = 0 # Points to the element we link to. Normally just the first element in the list which is the oldest.
    last = len(files) - 1 # Points to the next element we will remove and replace by a hardlink to files[base].
    startTime = time.time()
    numHardlinksInode = -1
    while base < last:
        if files[base].inode != numHardlinksInode:
            numHardlinks = getNumHardlinks(files[base].path)
            numHardlinksInode = files[base].inode
        while (base < last) and (numHardlinks < options.max_hardlinks):
            # Do not hardlink files again which are already hardlinked.
            if files[base].inode != files[last].inode:
                linkTwoFiles(files[base], files[last])
                numHardlinks += 1
            last -= 1

            if progressDue():
                done = base + len(files) - last - 1
                printProgressProcessingFiles(done, ", linking {}/{}, hardlinks {}/{}, base {}, last {}".format(done, len(files) - 1, numHardlinks, options.max_hardlinks, base, last))
        base += 1
        if progressDue():
            done = base + len(files) - last - 1
            printProgressProcessingFiles(done, ", linking {}/{}, hardlinks {}/{}, base {}, last {}".format(done, len(files) - 1, numHardlinks, options.max_hardlinks, base, last))


def breakHardlinks(files):
    """Break hardlinks or re-link until all files have <= options.max_hardlinks hardlinks.
    """
    tooHighIndex = 0
    linksAvailableIndex = 0
    numRelinked = 0
    numCopied = 0
    while True:
        # Find next file which has too many hardlinks.
        while (tooHighIndex < len(files)) and (getNumHardlinks(files[tooHighIndex].path) <= options.max_hardlinks):
            if progressDue():
                printProgressProcessingFiles(tooHighIndex, ", checking {}/{}, relinked {}, copies {}".format(tooHighIndex, len(files), numRelinked, numCopied))
            tooHighIndex += 1
        if tooHighIndex >= len(files):
            break # No more files have too many hardlinks. Done.

        # Find next file which has links available.
        while (linksAvailableIndex < len(files)) and (getNumHardlinks(files[linksAvailableIndex].path) >= options.max_hardlinks):
            linksAvailableIndex += 1
        if linksAvailableIndex >= len(files):
            # No more links available in existing files.
            # Break a link by making a copy, creating a new file with hardlink count of 1.
            breakHardlink(files[tooHighIndex])
            numCopied += 1
            # Restart.
            linksAvailableIndex = tooHighIndex
            tooHighIndex += 1
            continue

        # Link from linksAvailableIndex to tooHighIndex.
        linkTwoFiles(files[linksAvailableIndex], files[tooHighIndex])
        tooHighIndex += 1
        numRelinked += 1


def hashBench(hash):
    """Hash function benchmark.
    """
#    totalSize = 1024*1024*1024
    totalSize = 1024*1024*100
    blockSize = 65536
    data = "a".encode("ascii") * blockSize
    numBytes = 0

    startTime = time.time()
    while numBytes < totalSize:
        hash.update(data)
        numBytes += len(data)
    try:
        hash.hexdigest()
    except TypeError:
        pass
    elapsedTime = time.time() - startTime
    print("{:6.1f}MB/s ({:3} bits) {}".format(numBytes / 1024.0 / 1024.0 / elapsedTime, hash.digest_size * 8, hash.name))


def intWithUnit(s):
    """Convert string to int, allowing unit suffixes.

    This is used as 'type' for argparse.ArgumentParser.
    """
    if len(s) == 0:
        return int(s)
    index = "BkMGTPE".find(s[-1])
    if index >= 0:
        return int(float(s[:-1]) * (1 << (index * 10)))
    else:
        return int(s)


def main():
    """Main function of this module.
    """
    global options
    usage = """Usage: %(prog)s [OPTIONS] DIRS...
    """
    version = "0.1.0"
    parser = argparse.ArgumentParser(usage = usage + "\n(Version " + version + ")\n")
    parser.add_argument("args", nargs="*", help="Dirs to process.")
    parser.add_argument(      "--db", help="Database filename which stores all file attributes persistently between runs inside each dir.", type=str, default=".hardshrinkdb")
    parser.add_argument("-f", "--force-scan", help="Ignore any existing db files. Always scan directories and overwrite db files.", action="store_true", default=False)
    parser.add_argument("-u", "--update", help="Update existing db files by re-scanning the directories but not re-calculating the hashes if the inode, size and mtime did not change.", action="store_true", default=False)
    parser.add_argument(      "--grow", help="Create copies of files which have more hardlinks than specified with --max-hardlinks. Specify --grow --max-hardlinks 1 to break all hardlinks.", action="store_true", default=False)
    parser.add_argument(      "--ignore-dirs-without-db", help="Ignore dirs which do not already have a db file.", action="store_true", default=False)
    parser.add_argument("-0", "--dummy", help="Dummy mode. Nothing will be hardlinked, but db files will be created/updated.", action="store_true", default=False)
    parser.add_argument("-V", "--verbose", help="Be more verbose. May be specified multiple times.", action="count", default=0) # -v is taken by --version, argh!
    parser.add_argument(      "--quiet", help="Do not even print final stats.", action="store_true", default=False)
    parser.add_argument("-p", "--progress", help="Indicate progress.", action="store_true", default=False)
    parser.add_argument(      "--dump", help="Print DBs. Do not link/process anything further after scanning and/or reading dbs.", action="store_true", default=False)
    parser.add_argument("-D", "--print-duplicates", help="Print duplicate files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument(      "--print-singletons", help="Print singleton files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument(      "--print-all", help="Print all files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument("-W", "--progress-width", help="Width of the path display in the progress output.", type=int, default=100)
    parser.add_argument(      "--max-hardlinks", help="Maximum number of hardlinsk created per file. Must <= 65000 on Linux and <= 1023 on Windows.", type=int, default=55000)
    parser.add_argument(      "--reverse", help="Process smallest files first, going up. The default is process the biggest files first.", action="store_true", default=False)
    parser.add_argument(      "--min-size", help="Only process files >= min-size.", type=intWithUnit, default=0)
    parser.add_argument(      "--max-size", help="Only process files <= max-size.", type=intWithUnit, default=2**64)
    parser.add_argument(      "--hash-benchmark", help="Benchmark various hash algorithms, then exit.", action="store_true", default=False)
    options = parser.parse_args()
    options.db_bytes = strToBytes(options.db)

    if options.hash_benchmark:
        hashes =  sorted(set((x.lower() for x in hashlib.algorithms_available)))
        print(hashes)
        for hash in hashes:
            hashBench(hashlib.new(hash))
        return

    # Check args.
    if len(options.args) < 1:
        parser.error("Expecting at least one directory")
    if options.print_duplicates + options.print_singletons + options.print_all > 1:
        parser.error("Only one of the --print-* options may be specified.")
    if os.name == "nt":
        if options.max_hardlinks > 500:
            options.max_hardlinks = 500

    # Check all dirs beforehand to show errors fast.
    for i in options.args:
        if not os.path.exists(i):
            parser.error("{} does not exist".format(i))
        if not os.path.isdir(i):
            parser.error("{} is not a directrory".format(i))

    if options.dummy:
        print("Dummy mode: Not linking anything (but db files will potentially be created/updated).")

    try:
        # List of dbs.
        dbList = []

        # Scan all dirs or read the dbs.
        stats.numDirsTotalCmdLine = len(options.args)
        for dir in options.args:
            db = processDir(strToBytes(os.path.normpath(dir)))
            if db != None:
                dbList.append(db)
                stats.numDirsTotal += 1
                stats.numBytesInDb += db.getTotalDbSizeInBytes()
            stats.currentDir += 1

        if not options.dump:

            # Find and hardlink duplicates.
            func = linkFiles
            if options.grow:
                func = breakHardlinks
            if options.print_duplicates:
                func = printDuplicates
            if options.print_singletons:
                func = printSingletons
            if options.print_all:
                func = printAll
            findDuplicates(dbList, func)

            # Update db files in case files were hardlinked or hashed to update the inode, mtime and/or hash fields for these files.
            for db in dbList:
                if db.dirty:
                    db.save(db.dbfile)

    except RuntimeError as e:
        print("Error: {}".format(str(e)))
        return

    if (not options.quiet) and (not options.dump):
        stats.printStats()



# call main()
if __name__ == "__main__":
    main()

