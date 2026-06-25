echo 'fetching latest code...'
git pull
echo 'compiling...'
g++ tests/rpc.cpp rpc/event_loop/client.cpp rpc/event_loop/peer.cpp rpc/event_loop/event_loop.cpp core/node.cpp core/main_loop.cpp -std=c++23 -g -o unit
