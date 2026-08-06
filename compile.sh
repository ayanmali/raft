#!/bin/bash
echo 'fetching latest code...'
git pull
rm persistence/*
echo 'compiling...'
g++ tests/rpc.cpp -std=c++20 -Wno-interference-size -g -o unit
