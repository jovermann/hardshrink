// hardshrink - Deduplicate and hardlink files across huge directory trees
//
// Copyright (c) 2024-2025 Johannes Overmann
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

#include "MiscUtils.hpp"
#include "CommandLineParser.hpp"
#include "Hash.hpp"
#include "HashSha3.hpp"
#include <exception>
#include <iomanip>
#include <vector>
#include <span>
#include <list>
#include <map>

static unsigned clVerbose = 0;
static bool clDummy = false;
static bool clSameFilename = false;
static bool clProgress = false;

using FileSize = uint64_t;
using NumFiles = size_t;
using DirIndex = size_t;
using FileIndex = size_t;

uint64_t getUint64(const uint8_t* bytes)
{
    return bytes[0]
    | (uint64_t(bytes[1]) << 8)
    | (uint64_t(bytes[2]) << 16)
    | (uint64_t(bytes[3]) << 24)
    | (uint64_t(bytes[4]) << 32)
    | (uint64_t(bytes[5]) << 40)
    | (uint64_t(bytes[6]) << 48)
    | (uint64_t(bytes[7]) << 56);
}


struct Hash
{
    uint64_t lo;
    uint64_t hi;
};


static Hash getHash(const std::string& data)
{
    std::vector<uint8_t> b = calcHash<HashSha3_128>(data);
    Hash r;
    r.lo = getUint64(b.data() + 0);
    r.hi = getUint64(b.data() + 8);
    return r;
}


inline bool operator<(const Hash& a, const Hash& b)
{
    if (a.hi < b.hi)
    {
        return true;
    }
    if (a.hi > b.hi)
    {
        return false;
    }
    return a.lo < b.lo;
}

struct File
{
    File(const std::filesystem::directory_entry& entry)
    {
        size = entry.file_size();
        path = entry.path();
        hash = getHash(ut1::readFile(path));
    }

    Hash hash;
    uint64_t size;
    std::string path;
    uint64_t inode;
    uint64_t date;
    uint32_t numLinks;
};

/// DirDB file format:
/// uint64_t "DirDB" magic;
/// uint64_t version
/// uint64_t "TOC";
/// uint64_t totalSizeInBytes;
/// uint64_t entrySizeInBytes;
/// TocEntry tocEntries[];
/// uint64_t "FILES";
/// uint64_t totalSizeInBytes;
/// uint64_t entrySizeInBytes;
/// File files[];
/// uint64_t "STRINGS";
/// uint64_t totalSizeInBytes;
/// String sequence.
///   First byte;
///     0-0xfc: 1-byte length
///     0xff: 2-byte length follows
///     0xfe: 4-byte length follows
///     0xfd: 8-byte length follows
///   Perhaps 2-8-byte length (if first byte is 0xff, 0xfe or 0xfd)
///   String (not zero terminated)

/// TocEntry.
struct TocEntry
{
    uint64_t size;
    uint64_t fileIndex;
};






static void printWarning(const std::string& message)
{
    std::cout << message;
}


static void printVerbose(const std::string& message, unsigned level = 1)
{
    if (level <= clVerbose)
    {
        std::cout << message;
    }
}


static std::string percentStr(size_t num, size_t total)
{
    return std::format("{}/{} ({:.1f}%)", num, total, double(num) * 100.0 / double(total));
}


static const char *unit[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB"};


static std::string sizeStr(size_t size)
{
    double s = size;
    unsigned i = 0;
    while (s >= 1000.0)
    {
        s /= 1024.0;
        i++;
    }
    unsigned digits = 3;
    if (s >= 10.0)
    {
        digits = 2;
    }
    if (s >= 100.0)
    {
        digits = 1;
    }

    return std::format("{:.{}f}{}", s, digits, unit[i]);
}


static std::string percentSizeStr(size_t size, size_t total)
{
    double s = size;
    double t = total;
    unsigned i = 0;
    while (t >= 1000.0)
    {
        s /= 1024.0;
        t /= 1024.0;
        i++;
    }
    unsigned digits = 3;
    if (t >= 10.0)
    {
        digits = 2;
    }
    if (t >= 100.0)
    {
        digits = 1;
    }
    return std::format("{:.{}f}/{:.{}f}{} ({:.1f}%)", s, digits, t, digits, unit[i], s * 100.0 / t);
}


class DirDb
{
public:
    DirDb(const std::string& path_, size_t commandLineIndex_): path(path_), commandLineIndex(commandLineIndex_)
    {
        scanDir(path);
    }

    std::string getPath() const { return path; }

    /// @brief Get command line index of this directory.
    /// @return Comand line idex of this directory.
    size_t getCommandLineIndex() const { return commandLineIndex; }

    void printStats()
    {
        std::cout << std::format("{} contains {} files and {}\n", getPath(), getNumFiles(), sizeStr(getTotalBytes()));
    }

    /// @brief Print statistics comparing this dir to another dir db.
    /// @param otherDirDb Other dir db.
    void printCompareStats(DirDb& otherDirDb)
    {
        size_t numFilesInOther{};
        size_t numBytesInOther{};
        size_t numFilesNotInOther{};
        size_t numBytesNotInOther{};
        forEachFile([&](const File& file) {
            if (otherDirDb.hasHash(file.hash))
            {
                numFilesInOther++;
                numBytesInOther += file.size;
            }
            else
            {
                numFilesNotInOther++;
                numBytesNotInOther += file.size;
            }
        });

        std::cout << std::format("{}: {} files ({}) are contained in {}\n", getPath(), percentStr(numFilesInOther, getNumFiles()), percentSizeStr(numBytesInOther, getTotalBytes()), otherDirDb.getPath());
    }

    void forEachFile(auto func) const
    {
        for (const File& file: files)
        {
            func(file);
        }
    }

    bool hasHash(const Hash& hash) const
    {
        return hashToFileIndex.contains(hash);
    }

    size_t getNumFiles() const { return files.size(); }

    size_t getTotalBytes() const
    {
        size_t r = 0;
        forEachFile([&](const File& file) { r += file.size; });
        return r;
    }

private:
    void scanDir(const std::string& path_)
    {
        for (const std::filesystem::directory_entry& entry : std::filesystem::recursive_directory_iterator(path_))
        {
            if (entry.is_regular_file())
            {
                files.emplace_back(entry);
                HashToFileIndexMap::iterator i = hashToFileIndex.find(files.back().hash);
                if (i != hashToFileIndex.end())
                {
                    i->second.push_back(files.size() - 1);
                }
                else
                {
                    hashToFileIndex[files.back().hash] = std::vector<size_t>({files.size() - 1});
                }
            }
        }
    }

    // --- Private data. ---
    std::string path;
    size_t commandLineIndex{};
    std::vector<File> files;
    using HashToFileIndexMap = std::map<Hash,std::vector<size_t>>;
    HashToFileIndexMap hashToFileIndex;
};

class MainDb
{
public:
    MainDb() { }

    void addDir(const std::string& path, size_t commandLineIndex)
    {
        dirDbs.push_back(DirDb(path, commandLineIndex));
    }

    void printStats()
    {
        for (auto &dirDb: dirDbs)
        {
            if (dirDbs.size() == 1)
            {
                dirDb.printStats();
            }
            for (auto &dirDb2: dirDbs)
            {
                if (dirDb.getCommandLineIndex() != dirDb2.getCommandLineIndex())
                {
                    dirDb.printCompareStats(dirDb2);
                }
            }
        }
    }

private:
    /// Remove file.
    void removeFile(const std::string& path)
    {
        if (clVerbose)
        {
            printVerbose("Removing redudant file '" + path + "'.\n");
        }
        if (!clDummy)
        {
            std::filesystem::remove(path);
        }
    }

    // --- Private data. ---
    std::list<DirDb> dirDbs;
};


/// Main.
int main(int argc, char *argv[])
{
    // Command line options.
    const char *usage = "Deduplicate and hardlink files across huge directory trees.\n"
                        "\n"
                        "Usage: $programName [OPTIONS] DIR...\n"
                        "\n";
    ut1::CommandLineParser cl("hardshrink", usage,
        "\n$programName version $version *** Copyright (c) 2024-2025 Johannes Overmann *** https://github.com/jovermann/hardshrink",
        "0.0.1");

    cl.addHeader("\nOptions:\n");
    cl.addOption('S', "print-stats", "Print statistics about dirs and their relation in terms of containing the same content.");
    cl.addOption('s', "same-filename", "Only treat files with the same filename and the same content as the same file.");
    cl.addOption('d', "dummy", "Dummy mode. Nothing is written or removed, but .dirdb files will be created and updated.");
    cl.addOption('p', "progress", "Print progress information.");
    cl.addOption('v', "verbose", "Increase verbosity. Specify multiple times to be more verbose.");

    // Parse command line options.
    cl.parse(argc, argv);
    clVerbose = cl.getCount("verbose");
    clDummy = cl("dummy");
    clSameFilename = cl("same-filename");
    if (cl.getArgs().size() == 0)
    {
        cl.error("Expecting at least one dir to operate on.");
    }
    if (!(cl("print-stats")))
    {
        cl.setOption("print-stats");
    }

    try
    {
        MainDb mainDb;

        // Check all args to avoid late errors.
        for (const std::string& path : cl.getArgs())
        {
            if (!ut1::fsExists(path))
            {
                cl.error("Path '" + path + "' does not exist.");
            }
            if (!ut1::fsIsDirectory(path))
            {
                cl.error("Path '" + path + "' is not a directory.");
            }
        }

        // Create/read dirdbs.
        for (size_t i = 0; i < cl.getArgs().size(); i++)
        {
            mainDb.addDir(cl.getArgs()[i], i);
        }

        if (cl("print-stats"))
        {
            mainDb.printStats();
        }

        if (clVerbose)
        {
            std::cout << "Done.\n";
        }
    }
    catch (const std::exception& e)
    {
        cl.error(e.what());
    }

    return 0;
}
