#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-6379}"
PASS=0
FAIL=0

run() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    echo "FAIL: $desc (expected '$expected', got '$actual')"
  fi
}

redis-cli -p $PORT FLUSHALL > /dev/null

redis-cli -p $PORT SET k1 hello > /dev/null
run "SET/GET" "hello" "$(redis-cli -p $PORT GET k1)"

redis-cli -p $PORT SET k2 temp EX 100 > /dev/null
run "SET EX" "temp" "$(redis-cli -p $PORT GET k2)"
ttl=$(redis-cli -p $PORT TTL k2)
[ "$ttl" -gt 0 ] 2>/dev/null && run "TTL after SET EX" "pass" "pass" || run "TTL after SET EX" "positive" "$ttl"

redis-cli -p $PORT SET k1 other NX > /dev/null
run "SET NX no overwrite" "hello" "$(redis-cli -p $PORT GET k1)"

redis-cli -p $PORT SET counter 10 > /dev/null
redis-cli -p $PORT INCR counter > /dev/null
run "INCR" "11" "$(redis-cli -p $PORT GET counter)"
redis-cli -p $PORT DECR counter > /dev/null
run "DECR" "10" "$(redis-cli -p $PORT GET counter)"
redis-cli -p $PORT INCRBY counter 5 > /dev/null
run "INCRBY" "15" "$(redis-cli -p $PORT GET counter)"

redis-cli -p $PORT MSET m1 a m2 b m3 c > /dev/null
run "MSET/GET m1" "a" "$(redis-cli -p $PORT GET m1)"
run "MSET/GET m2" "b" "$(redis-cli -p $PORT GET m2)"

redis-cli -p $PORT SET app hello > /dev/null
redis-cli -p $PORT APPEND app " world" > /dev/null
run "APPEND" "hello world" "$(redis-cli -p $PORT GET app)"
run "STRLEN" "11" "$(redis-cli -p $PORT STRLEN app)"

redis-cli -p $PORT SET delme yes > /dev/null
redis-cli -p $PORT DEL delme > /dev/null
run "DEL/EXISTS" "0" "$(redis-cli -p $PORT EXISTS delme)"

redis-cli -p $PORT SET expkey val > /dev/null
redis-cli -p $PORT EXPIRE expkey 100 > /dev/null
ttl=$(redis-cli -p $PORT TTL expkey)
[ "$ttl" -gt 0 ] 2>/dev/null && run "EXPIRE/TTL" "pass" "pass" || run "EXPIRE/TTL" "positive" "$ttl"
redis-cli -p $PORT PERSIST expkey > /dev/null
run "PERSIST" "-1" "$(redis-cli -p $PORT TTL expkey)"

run "TYPE string" "string" "$(redis-cli -p $PORT TYPE k1)"

redis-cli -p $PORT RPUSH mylist a b c > /dev/null
run "LLEN" "3" "$(redis-cli -p $PORT LLEN mylist)"
run "LINDEX 0" "a" "$(redis-cli -p $PORT LINDEX mylist 0)"
run "LPOP" "a" "$(redis-cli -p $PORT LPOP mylist)"
run "RPOP" "c" "$(redis-cli -p $PORT RPOP mylist)"
run "TYPE list" "list" "$(redis-cli -p $PORT TYPE mylist)"

redis-cli -p $PORT HSET myhash name lux version 2 > /dev/null
run "HGET" "lux" "$(redis-cli -p $PORT HGET myhash name)"
run "HLEN" "2" "$(redis-cli -p $PORT HLEN myhash)"
run "HEXISTS yes" "1" "$(redis-cli -p $PORT HEXISTS myhash name)"
run "HEXISTS no" "0" "$(redis-cli -p $PORT HEXISTS myhash missing)"
run "TYPE hash" "hash" "$(redis-cli -p $PORT TYPE myhash)"

redis-cli -p $PORT SADD myset x y z > /dev/null
run "SCARD" "3" "$(redis-cli -p $PORT SCARD myset)"
run "SISMEMBER yes" "1" "$(redis-cli -p $PORT SISMEMBER myset x)"
run "SISMEMBER no" "0" "$(redis-cli -p $PORT SISMEMBER myset w)"
run "TYPE set" "set" "$(redis-cli -p $PORT TYPE myset)"

redis-cli -p $PORT SETNX nxk v1 > /dev/null
redis-cli -p $PORT SETNX nxk v2 > /dev/null
run "SETNX" "v1" "$(redis-cli -p $PORT GET nxk)"

redis-cli -p $PORT SET gskey old > /dev/null
run "GETSET returns old" "old" "$(redis-cli -p $PORT GETSET gskey new)"
run "GETSET sets new" "new" "$(redis-cli -p $PORT GET gskey)"

redis-cli -p $PORT SET ren1 val > /dev/null
redis-cli -p $PORT RENAME ren1 ren2 > /dev/null
run "RENAME" "val" "$(redis-cli -p $PORT GET ren2)"

redis-cli -p $PORT HSET hincr count 10 > /dev/null
redis-cli -p $PORT HINCRBY hincr count 5 > /dev/null
run "HINCRBY" "15" "$(redis-cli -p $PORT HGET hincr count)"

run "PING" "PONG" "$(redis-cli -p $PORT PING)"

size=$(redis-cli -p $PORT DBSIZE)
[ "$size" -gt 0 ] 2>/dev/null && run "DBSIZE" "pass" "pass" || run "DBSIZE" "positive" "$size"

redis-cli -p $PORT FLUSHALL > /dev/null

echo ""
echo "=== RESULTS ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo "Total: $((PASS + FAIL))"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
