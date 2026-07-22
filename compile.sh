echo 'fetching latest code...'
git pull
echo 'compiling...'
g++ tests/rpc.cpp -std=c++23 -Wno-interference-size -g -o unit
