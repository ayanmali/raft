#!/bin/bash
g++ -std=c++20 -O3 -g -march=native -fno-omit-frame-pointer -Wno-interference-size -Wno-stringop-overflow tests/rpc.cpp -o prof
