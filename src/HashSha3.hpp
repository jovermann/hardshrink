// SHA-3 implementation.
//
// Copyright (c) 2024 Johannes Overmann
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <stdint.h>
#include <vector>
#include <algorithm>
#include <bit>

/// SHA-3 implementation according to FIPS PUB 202.
/// https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.202.pdf
/// Please use class HashSha3_128, HashSha3_224, HashSha3_256, HashSha3_384 or HashSha3_512 etc instead (see bottom of file).
class HashSha3
{
public:
    HashSha3(size_t hashSizeInBits = 224):
    hashSizeBytes(hashSizeInBits / 8),
    blockSizeBytes(200 - 2 * hashSizeBytes)
    {
        clear();
    }

    /// Initialize hasher.
    /// Call this after retrieving the hash and before calculating a new hash of new data.
    void clear()
    {
        std::fill(state, state + 25, 0);
        bufferPos = 0;
    }

    /// Add data.
    void update(const uint8_t *bytes, size_t n)
    {
        uint8_t *state8 = reinterpret_cast<uint8_t*>(state);
        for (size_t i = 0; i < n;)
        {
            if (((bufferPos & 7) == 0) && ((n - i) >= 8) && ((blockSizeBytes - bufferPos) >= 8))
            {
                for (;((n - i) >= 8) && ((blockSizeBytes - bufferPos) >= 8); bufferPos += 8, i += 8)
                {
                    state[bufferPos >> 3] ^= *reinterpret_cast<const uint64_t*>(&bytes[i]);
                }
            }
            else if ((i < n) && (bufferPos < blockSizeBytes))
            {
                state8[bufferPos++] ^= bytes[i++];
            }
            if (bufferPos >= blockSizeBytes)
            {
                processBlock();
                bufferPos = 0;
            }
        }
    }

    /// Get hash.
    std::vector<uint8_t> finalize()
    {
        uint8_t *state8 = reinterpret_cast<uint8_t*>(state);
        state8[bufferPos] ^= 0x06;
        state8[blockSizeBytes - 1] ^= 0x80;
        processBlock();

        return std::vector<uint8_t>(state8, state8 + hashSizeBytes);
    }

private:
    void processBlock()
    {
#define HashSha3_REPEAT5(x) x x x x x
#define HashSha3_REPEAT24(x) x x x x x x x x x x x x x x x x x x x x x x x x
#define HashSha3_FOR5(var, step, code) var = 0; HashSha3_REPEAT5(code; var += step;)
#define HashSha3_FOR24(var, step, code) var = 0; HashSha3_REPEAT24(code; var += step;)
        uint64_t c[5];
        unsigned round, i, j;
        HashSha3_FOR24(round, 1,
            // Theta.
            HashSha3_FOR5(i, 1, c[i] = state[i] ^ state[i + 5] ^ state[i + 10] ^ state[i + 15] ^ state[i + 20];)
            HashSha3_FOR5(i, 1, HashSha3_FOR5(j, 5, state[j + i] ^= c[(i + 4) % 5] ^ std::rotl(c[(i + 1) % 5], 1);))

            // Rho and Pi.
            c[1] = state[1];
            HashSha3_FOR24(i, 1, j = piOffsets[i]; c[0] = state[j]; state[j] = std::rotl(c[1], rhoRotate[i]); c[1] = c[0];)

            // Chi.
            HashSha3_FOR5(j, 5, HashSha3_FOR5(i, 1, c[i] = state[j + i];) HashSha3_FOR5(i, 1, state[j + i] ^= (~c[(i + 1) % 5]) & c[(i + 2) % 5];))

            // Iota.
            state[0] ^= iota[round];
        )
#undef HashSha3_REPEAT5
#undef HashSha3_REPEAT24
#undef HashSha3_FOR5
#undef HashSha3_FOR24
    }

    /// Iota constants.
    static constexpr uint64_t iota[24] =
    {
        0x0000000000000001, 0x0000000000008082, 0x800000000000808a, 0x8000000080008000,
        0x000000000000808b, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
        0x000000000000008a, 0x0000000000000088, 0x0000000080008009, 0x000000008000000a,
        0x000000008000808b, 0x800000000000008b, 0x8000000000008089, 0x8000000000008003,
        0x8000000000008002, 0x8000000000000080, 0x000000000000800a, 0x800000008000000a,
        0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008
    };

    /// Rho rotation.
    static constexpr unsigned rhoRotate[24] = { 1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44};

    /// Pi permutation offsets.
    static constexpr unsigned piOffsets[24] = { 10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1};

    /// State.
    uint64_t state[25];

    /// Hash size in bytes.
    size_t hashSizeBytes;

    /// Block size in bytes.
    size_t blockSizeBytes;

    /// Byte position in state buffer.
    size_t bufferPos;
};

/// SHA-3 variants for the defined hash sizes.
class HashSha3_128: public HashSha3 { public: HashSha3_128(): HashSha3(128) {} }; // Non-standard, but fast
class HashSha3_224: public HashSha3 { public: HashSha3_224(): HashSha3(224) {} };
class HashSha3_256: public HashSha3 { public: HashSha3_256(): HashSha3(256) {} };
class HashSha3_384: public HashSha3 { public: HashSha3_384(): HashSha3(384) {} };
class HashSha3_512: public HashSha3 { public: HashSha3_512(): HashSha3(512) {} };
