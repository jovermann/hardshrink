// Interface of all hash implementations and common helper functions.
//
// Copyright (c) 2024 Johannes Overmann
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <vector>
#include <string>
#include <stdint.h>

/// Get hash of bytes.
template <class HASH>
std::vector<uint8_t> calcHash(const uint8_t *bytes, size_t n)
{
    HASH hasher;
    hasher.update(bytes, n);
    return hasher.finalize();
}

/// Get hash of string.
template <class HASH>
std::vector<uint8_t> calcHash(const std::string &s)
{
    return calcHash<HASH>(reinterpret_cast<const uint8_t *>(s.data()), s.length());
}

/// Get hash of bytes.
template <class HASH>
std::vector<uint8_t> calcHash(const std::vector<uint8_t> &bytes)
{
    return calcHash<HASH>(bytes.data(), bytes.size());
}

/// Add string to hash.
template<class HashClass>
void updateHash(HashClass &hasher, const std::string &s)
{
    hasher.update(reinterpret_cast<const uint8_t *>(s.data()), s.length());
}

/// Add bytes to hash.
template<class HashClass>
void updateHash(HashClass &hasher, const std::vector<uint8_t> &bytes)
{
    hasher.update(reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size());
}
