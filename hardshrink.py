#!/usr/bin/python
#
# hardshrink.py - find duplicate files and create hardlinks
#
# Copyright (C) 2019 by Johannes Overmann <Johannes.Overmann@joov.de>

import argparse
import hashlib
import os
import stat
import sys
import time


# Todo:
# - progress for size (x/y)
# - performance in MB/s for hashing


# Command line options.
options = None

lastProgressTime = 0.0


class Error:
    """Error exception.
    """
    def __init__(self, message):
        """Constructor.
        """
        self.message = message


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
        raise Error("kb(x) for x < 0 called")
    if width < 5:
        raise Error("kb(x,w) for w < 5 called")
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
        self.numFilesTotal = 0
        self.numBytesTotal = 0


    def printStats(self):
        """
        """
        for k, v in sorted(self.__dict__.iteritems()):
            if not k.startswith("_"):
                if k.find("Bytes") >= 0:
                    print("{:20}= {}".format(k, kB(v)))
                else:
                    print("{:20}= {:13}".format(k, v))


stats = Stat()
        

class Entry(dict):
    """File attributes.
    """ 
    def __init__(self):
        """Constructor.
        """
        self.path = None
        self.inode = None # Filled by stat.
        self.size = None  # Filled by stat.
        self.mtime = None # Filled by stat.
        self.hash = ""    # Optional. Filled in on demand.


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


def readDb(path):
    """Read db from file.
    """
    if options.verbose >= 1:
        print("reading db {} ...".format(path))
    db = []
    i = 0
    pathdir = os.path.dirname(path)
    with open(path, "rb") as f:
        for line in f:
            if progressDue():
               printProgress("{:6} files".format(i))
            i += 1
            fields = line.rstrip("\n").split(",", 4)
            if len(fields) != 5:
                print("Error: Ignoring corrupt line {}".format(line))
            else:
                try:
                    entry = Entry()
                    entry.size = int(fields[0])
                    entry.inode = int(fields[1])
                    entry.mtime = float(fields[2])
                    entry.hash = fields[3]
                    entry.path = fields[4][1:-1]
                except ValueError:
                    print("Error: Ignoring corrupt line {} (ValueError)".format(line))
                    continue
                entry.path = os.path.join(pathdir, entry.path)
                db.append(entry)
    if options.verbose >= 1:
        print("read {} files from {}".format(len(db), path))
    return db


def writeDb(path, db):
    """Write db to file.
    """
    if options.verbose >= 2:
        print("writing {} files to {}".format(len(db), path))
    tmppath = path + ".tmp"
    i = 0
    with open(tmppath, 'wb') as f:
        for entry in db:
            if progressDue():
               printProgress("{:6} files".format(i))
            i += 1
            relpath = os.path.relpath(entry.path, os.path.dirname(path))
            f.write("{entry.size:10d},{entry.inode:10d},{entry.mtime:f},{entry.hash},\"{relpath}\"\n".format(entry=entry, relpath=relpath))
    os.rename(tmppath, path)

                
def scanDir(dir):
    """Scan directory recursively.
    """
    if options.verbose >= 1:
        print("scanning dir {}".format(dir))
    db = []
    size = 0
    for root, dirs, files in os.walk(dir):
        if options.verbose >= 2:
            print("dir {}".format(root))
        for f in files:
            path = os.path.join(root, f)
            statinfo = os.lstat(path)
            if not stat.S_ISREG(statinfo.st_mode):
                continue
            if f == options.db:
                continue
            if options.verbose >= 3:
                print("file {}".format(path))
            entry = Entry()
            entry.path = path
            entry.inode = statinfo.st_ino
            entry.size = statinfo.st_size
            entry.mtime = statinfo.st_mtime
            db.append(entry)
            size += roundUpBlocks(entry.size)
            if progressDue():
                printProgress("scan {:6d} {} {}".format(len(db), kB(size), path[-options.progress_width:]))
    if options.verbose >= 1:
        print("scanned {} files ({}) {}".format(len(db), kB(size), " " * options.progress_width))
    return db


def processDir(dir):
    """Process directory.
    """
    dbfile = os.path.join(dir, options.db)
    if (not options.force_scan) and os.path.isfile(dbfile):
        return readDb(dbfile)
    else:
        db = scanDir(dir)
        writeDb(dbfile, db)
        return db


def getHash(path):
    """Get hash for file.
    
    We use sha1 since this is the fastest algorithm in hashlib (except for 
    md4). The current collision attacks are of no concern for the purpose
    of discriminating (just) 10**12 different files or less.
    """
    if options.verbose >= 2:
        print("hashing {}".format(path))

    blocksize=65536
    hash = hashlib.sha1()
    numBytes = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
            numBytes += len(block)
    stats.numBytesHashed += numBytes
    stats.numFilesHashed += 1
    return hash.hexdigest()


def collapseTwoFiles(a, b):
    """Collapse two files.
    """
    # Sanity checks.
    if id(a) == id(b):
        raise Error("Internal error: collapseTwoFiles() on the same file!")
    if (a.hash == "") or (b.hash == ""):
        raise Error("Internal error: collapseTwoFiles() files have not been hashed!")
    if a.hash != b.hash:
        raise Error("Internal error: collapseTwoFile() on files with different hashes!")
    if a.size != b.size:
        raise Error("Internal error: collapseTwoFile() on files with different size!")
    if a.inode == b.inode:
        raise Error("Internal error: collapseTwoFile() on files with the same inode!")
    # This always holds because files are sorted by ascending mtime.
    if a.mtime > b.mtime:
        raise Error("Internal error: collapseTwoFile(): First file must be older than second!")

    # Hardlink a to b.
    if options.verbose >= 2:
        prefix = os.path.commonprefix([a.path, b.path])
        pos = prefix.rfind("/")
        if pos != -1:
            prefix = prefix[:pos + 1]
        print("link {}/{} -> {}".format(prefix, a.path[len(prefix):], b.path[len(prefix):]))
    if not options.dummy:
        os.unlink(b.path)
        os.link(a.path, b.path)
        b.inode = a.inode
        b.mtime = a.mtime


def collapseFiles(db, frmIndex, toIndexList):
    """Collapse files.
    """
    for i in toIndexList:
        collapseTwoFiles(db[frmIndex], db[i])


def getSizeToIndexList(db):
    """Generate map from size to index list.
    """
    if options.verbose >= 2:
        print("generating map size -> index list")
    size2indexlist = dict()    
    for i in range(len(db)):
        size = db[i].size
        if size in size2indexlist:
            size2indexlist[size].append(i)
        else:
            size2indexlist[size] = [i]
    return size2indexlist

            
def getInodeToIndexList(db):
    """Generate map from inode to index list.
    """
    if options.verbose >= 2:
        print("generating map inode -> index list")
    inode2indexlist = dict()        
    for i in range(len(db)):
        if db[i].inode in inode2indexlist:
            inode2indexlist[db[i].inode].append(i)
        else:
            inode2indexlist[db[i].inode] = [i]
    return inode2indexlist
        
            
def consolidateHashes(db, inode2indexlist):
    """Make sure all entries for an inode have the same hash.
    
    And assign a hash for an entry when it is already known for that inode.
    """
    if options.verbose >= 2:
        print("consolidating hashes for all inodes")
    for inode, indexlist in inode2indexlist.iteritems():
        hashes = sorted(list(set((db[i].hash for i in indexlist))))
        if len(hashes) == 1:
            if len(hashes[0]) == 0:
                # Hash unknown: OK
                continue
            else:
                # Hash unique: OK
                continue
        elif len(hashes) == 2:
            if len(hashes[0]) == 0:
                # Hash unknown to some entries: OK. Assign known hash to all entries.
                for i in indexlist:
                    db[i].hash = hashes[1]
                continue
        # Hash not unique: Error
        raise Error("hashes not unique for inode")
            
            
def findDuplicates(db):
    """Find duplicates.
    """
    # Local statistics for progress.
    currentBytes = 0
    totalBytes = sum((entry.size for entry in db))
    startTime = time.time()
    
    size2indexlist = getSizeToIndexList(db)
    inode2indexlist = getInodeToIndexList(db)
    consolidateHashes(db, inode2indexlist)

    stats.numFilesTotal = len(db)
    stats.numBytesTotal = sum((roundUpBlocks(db[indexlist[0]].size) for inode, indexlist in inode2indexlist.iteritems()))
            
    # For all sizes in ascending order.
    for size, lst in sorted(size2indexlist.iteritems()):
        if size == 0: # No space saved by hardlinking zero length files.
            continue 
        if (len(lst) == 1) and not options.hash_all:
            continue # Just one file. Nothing to link.
        
        if options.verbose >= 2:
            print("processing size {:10} ({} files)".format(size, len(lst)))

        # Get list of unique inodes for size sorted by mtime.
        inodelist = list(set((db[i].inode for i in lst)))
        inodelist.sort(key = lambda inode: db[inode2indexlist[inode][0]].mtime)

        # Get map from hash to inode list.
        hash2inodelist = dict()
        for inode in inodelist:
            # Get hash for inode.
            hash = db[inode2indexlist[inode][0]].hash
            if hash == "":
                # Calc hash for inode.
                hash = getHash(db[inode2indexlist[inode][0]].path)
                # Assign hash to all files for that inode.
                for i in inode2indexlist[inode]:
                    if (db[i].hash != "") and (db[i].hash != hash):
                        raise Error("Internal error: Inconsistent hash!")
                    db[i].hash = hash
            
            if hash in hash2inodelist:
                hash2inodelist[hash].append(inode)
            else:
                hash2inodelist[hash] = [inode]

            currentBytes += db[inode2indexlist[inode][0]].size * len(inode2indexlist[inode])
            if progressDue():
                donePercent = currentBytes * 100.0 / totalBytes
                path = db[inode2indexlist[inode][0]].path
                printProgress("hash {}/{} ({:4.1f}%) {} {}".format(kB(currentBytes), kB(totalBytes), donePercent, remainingTimeStr(startTime, currentBytes, totalBytes), path[-options.progress_width:]))

        # For each uniqe hash.
        for hash, inodelist in hash2inodelist.iteritems():
            if len(inodelist) == 1:
                continue
            frm = inode2indexlist[inodelist[0]][0]
            toList = []
            for inode in inodelist[1:]:
                toList += inode2indexlist[inode]
                stats.numBytesRemoved += roundUpBlocks(db[frm].size)
            stats.numFilesRemoved += len(toList)
            collapseFiles(db, frm, toList)

            
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
    hash.hexdigest()
    elapsedTime = time.time() - startTime
      
    print("{:6.1f}MB/s ({:3} bits) {}".format(numBytes / 1024.0 / 1024.0 / elapsedTime, hash.digest_size * 8, hash.name))
            

def main():
    """Main function of this module.
    """
    global options
    usage = """Usage: %(prog)s [OPTIONS] DIRS...
    """
    version = "0.0.1"
    parser = argparse.ArgumentParser(usage = usage + "\n(Version " + version + ")\n")
    parser.add_argument("args", nargs="*", help="Dirs to process.")
    parser.add_argument("-d", "--db", help="Database which stores all file attributes persistently between runs.", type=str, default=".hardshrinkdb.csv")
    parser.add_argument("-B", "--block-size", help="Block size of underlying filesystem. Default 4096.", type=int, default=4096)
    parser.add_argument("-f", "--force-scan", help="Ignore any existing db files. Always scan directories and overwrite db files.", action="store_true", default=False)
    parser.add_argument("-0", "--dummy", help="Dummy mode. Nothing will be hardlinked, but db files will be created/overwritten.", action="store_true", default=False)
    parser.add_argument("-V", "--verbose", help="Be more verbose. May be specified multiple times.", action="count", default=0) # -v is taken by --version, argh!
    parser.add_argument("-p", "--progress", help="Indicate progress.", action="store_true", default=False)
    parser.add_argument("-W", "--progress-width", help="Width of the path display in the progress output.", type=int, default=100)
    parser.add_argument(      "--hash-benchmark", help="Benchmark various hash algorithms, then exit.", action="store_true", default=False)
    parser.add_argument("-A", "--hash-all", help="Hash all files, even when not necessary because they have a unique size.", action="store_true", default=False)
    options = parser.parse_args()

    if options.hash_benchmark:
        hashes =  sorted(set((x.lower() for x in hashlib.algorithms_available)))
        print(hashes)
        for hash in hashes:
            hashBench(hashlib.new(hash))
        return
    
    
    if False:
        for bits in range(0,80):
#            n = 1023 * 2 ** bits
            n = 2 ** bits
            print(kB(n, 7))
#            print(kB(n - 1, 5))
        return

    # Check args
    if len(options.args) < 1:
        parser.error("Expecting at leats one directory")
    # Check all dirs beforehand to show errors fast
    for i in options.args:
        if not os.path.exists(i):
            parser.error("{} does not exist".format(i))
        if not os.path.isdir(i):
            parser.error("{} is not a directrory".format(i))

    try:
        # File db.
        db = []

        # Map from dir to (startindex,endindex_exclusive) into db.
        dir2startEnd = dict()
        
        # Scan all dirs or read the dbs.
        for dir in options.args:
            startindex = len(db)
            db += processDir(dir)
            endindex = len(db)
            dir2startEnd[dir] = (startindex, endindex)
    
        # Find and hardlink duplicates.
        findDuplicates(db)
        
        # Update hashes in db files.
        for dir, v in dir2startEnd.iteritems():
            writeDb(os.path.join(dir, options.db), db[v[0]:v[1]])
            
    except Error as e:
        print("Error: {}".format(e.message))

    stats.printStats()
    
    
      
# call main()
if __name__ == "__main__":
    main()

