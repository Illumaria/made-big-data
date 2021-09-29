#!/usr/bin/env python3
"""reducer_variance.py"""

import sys


if __name__ == '__main__':
    cur_chunk_size, cur_mean, cur_variance = 0, 0, 0

    for line in sys.stdin:
        chunk_size, chunk_mean, chunk_variance = map(float, line.strip().split())

        cur_variance = (chunk_size * chunk_variance + cur_chunk_size * cur_variance) \
                       / (chunk_size + cur_chunk_size) \
                       + chunk_size * cur_chunk_size \
                       * ((chunk_mean - cur_mean) / (chunk_size + cur_chunk_size)) ** 2
        cur_mean = (chunk_size * chunk_mean + cur_chunk_size * cur_mean) \
                   / (chunk_size + cur_chunk_size)
        cur_chunk_size += chunk_size

    print(cur_variance)
