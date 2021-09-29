#!/usr/bin/env python3
"""reducer_mean.py"""

import sys


if __name__ == '__main__':
    cur_mean, cur_chunk_size = 0, 0

    for line in sys.stdin:
        chunk_size, chunk_mean = map(float, line.strip().split())

        cur_mean = (chunk_size * chunk_mean + cur_chunk_size * cur_mean) \
                   / (chunk_size + cur_chunk_size)
        cur_chunk_size += chunk_size

    print(cur_mean)
