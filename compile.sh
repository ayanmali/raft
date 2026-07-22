echo 'fetching latest code...'
git pull
echo 'compiling...'
g++ tests/rpc.cpp -std=c++20 -Wno-interference-size -g -o unit
