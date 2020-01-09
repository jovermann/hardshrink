#!/usr/bin/python3
#
# hardshrink.py - find duplicate files and create hardlinks
#
# Requires Python 3.5 (for os.path.commonpath())
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

class Stat:
    """Global statistics.
    """
    def __init__(self):
        """Constructor.
        """
        self.numFilesRemoved = 0
        self.numBytesRemoved = 0
        self.numFilesHashed = 0
        self.numBytesHashed = 0
        self.numFilesRedundant = 0
        self.numBytesRedundant = 0
        self.numFilesTotal = 0
        self.numBytesTotal = 0
        self.numFilesSingletons = 0
        self.numFilesGroups = 0
        self.numFilesSkipped = 0


    def printStats(self):
        """Print statistics.
        """
        print("Statistics:")
        for k, v in sorted(self.__dict__.items()):
            if not k.startswith("_"):
                if k.find("Bytes") >= 0:
                    print("{:20}= {}".format(k, kB(int(v))))
                else:
                    print("{:20}= {:13}".format(k, int(v)))


stats = Stat()


def strToBytes(filename):
    """Return bytes for a str filename.
    """
    return filename.encode("utf8")


def bytesToStr(filename):
    """Return str for a bytes filename.
    """
    return filename.decode("utf8")


def formatFloat(f, width):
    """Format positive floating point number into width chars.

    width shoule be >= 3 and must be >= 1.
    f should be < 10000.0
    """
    if f < 10.0:
        if width >= 3:
            return "{:{w}.{p}f}".format(f, w = width, p = width - 2)
    elif f < 100.0:
        if width >= 4:
            return "{:{w}.{p}f}".format(f, w = width, p = width - 3)
    elif f < 1000.0:
        if width >= 5:
            return "{:{w}.{p}f}".format(f, w = width, p = width - 4)
    elif f < 10000.0:
        if width >= 6:
            return "{:{w}.{p}f}".format(f, w = width, p = width - 5)
    return "{:{w}.0f}".format(f, w = width)


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
    if elapsedTime == 0.0:
        elapsedTime = 0.0001
    totalTime = (elapsedTime / current) * total
    remainingTime = totalTime - elapsedTime
    return time.strftime('%H:%M:%S', time.gmtime(remainingTime))


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


def getHash(path):
    """Get hash for file.

    We use sha1 since this is the fastest algorithm in hashlib (except for
    md4). The current collision attacks are of no concern for the purpose
    of discriminating (just) 10**12 different files or less.
    """
    if options.verbose >= 2:
        print("Hashing {}".format(bytesToStr(path)))

    blocksize = 65536
    hash = hashlib.sha1()
    numBytes = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
            numBytes += len(block)
    stats.numBytesHashed += numBytes
    stats.numFilesHashed += 1
    return hash.digest()


def roundUp(x, granularity):
    """Round up x to the next multiple of granularity.
    """
    return ((x + granularity - 1) / granularity) * granularity


def roundUpBlocks(x):
    """Round up to blocks used.
    """
    return roundUp(x, options.block_size)


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


def printProgress(s):
    """Print progress string.
    """
    if not options.progress:
        return
    sys.stdout.write(" ")
    sys.stdout.write(s)
    sys.stdout.write("    \r")
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
        self.iterator = 0


    def isIteratorValid(self):
        """Return True iff iterator is valid.
        """
        return self.iterator * self.entrySize < len(self.data)


    def getCurrentItem(self, advance = False):
        """Get current item under iterator, optionally incrementing iterator.

        This returns an Entry.
        """
        r = self.getEntry(self.iterator)
        if advance:
            self.iterator += 1
        return r


    def getCurrentItemKey(self):
        """Get current sort key for item hash under iterator.

        We intentionally return the full entry, not just the hash, because this
        key is used to sort HardshrinkDB containers during the merge-sort, and
        these must be sorted in the same way as sortarray.sortArray does, and
        this also uses the full entry.
        """
        if self.isIteratorValid():
            offset = self.iterator * self.entrySize
            return self.data[offset : offset + self.entrySize]
        else:
            # Return the highest hash value to make sure that DBs which ran
            # out of items are sorted to the end by getNextFileListWithTheSameHash()
            # so they can get removed from the DB list.
            return array.array("Q", (0xffffFFFFffffFFFF,) * self.entrySize)


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
            return 0xffffFFFFffffFFFF


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
            print("Reading db from {}".format(bytesToStr(filename)))
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


    def save(self, filename):
        """Save hardshrink db to file.
        """
        if options.verbose >= 1:
            print("Writing db to {} ({})".format(bytesToStr(filename), kB(self.getTotalSizeInBytes())))
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
        for root, dirs, files in os.walk(dir):
            if options.verbose >= 2:
                print("Dir {}".format(bytesToStr(root)))
            subdir = root[len(dir) + 1:]
            dirOffset = addString(self.stringData, subdir)
            for f in files:
                # Get meta-data.
                path = os.path.join(root, f)
                statinfo = os.lstat(path)
                if not stat.S_ISREG(statinfo.st_mode):
                    stats.numFilesSkipped += 1
                    continue
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

                totalSize += roundUpBlocks(size)
                if progressDue():
                    printProgress("Scan {:6d} {} {}".format(len(self.data) // self.entrySize), kB(totalSize), path[-options.progress_width:])
        if options.verbose >= 1:
            print("Scanned {} files ({}) {}".format(len(self.data) // self.entrySize, kB(totalSize), " " * options.progress_width))

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
        hash = getHash(path)
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


    def getTotalSizeInBytes(self):
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
        bytesPerFile = self.getTotalSizeInBytes() / self.getNumFiles()
        print("({} files, {} total, {} data, {} strings, {:.1f} bytes/file)".format(self.getNumFiles(), kB(self.getTotalSizeInBytes()), kB(len(self.data) * 4), kB(len(self.stringData)), bytesPerFile))
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
    dbList.sort(key = lambda x: x.getCurrentItemSize())
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
                # Entries with identical mtime:
                if entry.hasHash():
                    if inodeToEntry[entry.inode].hasHash():
                        if entry.hash != inodeToEntry[entry.inode].hash:
                            # Inconsistent hashes for the same inode an the same mtime: This indicates trouble and is worth a warning.
                            # To be conservative we remove the hashes from both entries since we do not know which one to trust.
                            print("Warning: Inconsistent hashes:")
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

    # Calculate missing hashes for all inodes which do not yet have a hash.
    for (inode, entry) in inodeToEntry.items():
        if not entry.hasHash():
            entry.calcHash()

    # Update the hashes of all files according to the map.
    for entry in files:
        if not entry.hasHash():
            entry.setHashAndMtime(inodeToEntry[entry.inode].hash, inodeToEntry[entry.inode].mtime)
        else:
            if entry.hash != inodeToEntry[entry.inode].hash:
                raise RuntimeError("Internal error: Inconsistent hashes!")

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
    while len(dbList) > 0:
        files = getNextFileListWithTheSameSize(dbList)
        fileLists = getListOfFileListsWithIdenticalHashes(files, justPropagateExistingHashes)
        if justPropagateExistingHashes:
            continue
        if len(fileLists) == 0:
            break

        for files in fileLists:
            # Process identical files.
            if len(files) == 1:
                stats.numFilesSingletons += 1
            else:
                stats.numFilesGroups += 1
                inodes = set([files[0].inode])
                for f in files[1:]:
                    if f.inode not in inodes:
                        stats.numFilesRedundant += 1
                        stats.numBytesRedundant += f.size
                        inodes.add(f.inode)
            stats.numFilesTotal += len(files)
            stats.numBytesTotal += sum((x.size for x in files))

            func(files)


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
            # Thus we need to rescan it completely.
            # But we want to avoid re-calculating the hashes for files which have
            # the same size and mtime, so take these from the old db.
            dbnew = HardshrinkDb()
            dbnew.scanDir(dir)
            dbList = [db, dbnew]
            findDuplicates(dbList, None, justPropagateExistingHashes = True)
            db = dbnew
            db.save(dbfile)
    else:
        # Fresh scan of directory. Ignore any existing db file.
        db.scanDir(dir)
        db.save(dbfile)
    if options.dump:
        db.dump()
    return db


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
        os.unlink(b.path)
        if not os.path.exists(b.path):
            stats.numBytesRemoved += b.size
            stats.numFilesRemoved += 1
        os.link(a.path, b.path)
        b.setInodeMtime(a.inode, a.mtime)


def linkFiles(files):
    """Link files to first file (which is supposed to be the oldest inode).
    """
    base = files[0]
    for entry in files[1:]:
        # Do not hardlink files again which are already hardlinked.
        if entry.inode == base.inode:
            continue
        linkTwoFiles(base, entry)


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


def main():
    """Main function of this module.
    """
    global options
    usage = """Usage: %(prog)s [OPTIONS] DIRS...
    """
    version = "0.0.3"
    parser = argparse.ArgumentParser(usage = usage + "\n(Version " + version + ")\n")
    parser.add_argument("args", nargs="*", help="Dirs to process.")
    parser.add_argument(      "--db", help="Database filename which stores all file attributes persistently between runs inside each dir.", type=str, default=".hardshrinkdb")
    parser.add_argument("-B", "--block-size", help="Block size of underlying filesystem. Default 4096.", type=int, default=4096)
    parser.add_argument("-f", "--force-scan", help="Ignore any existing db files. Always scan directories and overwrite db files.", action="store_true", default=False)
    parser.add_argument("-u", "--update", help="Update existing db files by re-scanning the directories but not re-calculating the hashes if the size and mtime did not change.", action="store_true", default=False)
    parser.add_argument("-0", "--dummy", help="Dummy mode. Nothing will be hardlinked, but db files will be created/updated.", action="store_true", default=False)
    parser.add_argument("-V", "--verbose", help="Be more verbose. May be specified multiple times.", action="count", default=0) # -v is taken by --version, argh!
    parser.add_argument("-p", "--progress", help="Indicate progress.", action="store_true", default=False)
    parser.add_argument(      "--dump", help="Print DBs. Do not link/process anything further after scanning and/or reading dbs.", action="store_true", default=False)
    parser.add_argument("-D", "--print-duplicates", help="Print duplicate files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument(      "--print-singletons", help="Print singleton files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument(      "--print-all", help="Print all files. Do not hardlink anything.", action="store_true", default=False)
    parser.add_argument("-W", "--progress-width", help="Width of the path display in the progress output.", type=int, default=100)
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
        for dir in options.args:
            dbList.append(processDir(strToBytes(os.path.normpath(dir))))

        if not options.dump:

            # Find and hardlink duplicates.
            func = linkFiles
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

    if options.verbose:
        stats.printStats()



# call main()
if __name__ == "__main__":
    main()

