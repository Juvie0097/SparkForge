#!/usr/bin/env bash
# 本地全流程测试脚本（laptop 模式）
# 用法：bash scripts/test_local.sh
set -euo pipefail

PASS=0; FAIL=0
run() {
  local label="$1"; shift
  printf "%-40s " "$label ..."
  if sbt "$@" > /tmp/sparkforge_out.txt 2>&1; then
    echo "PASS"
    PASS=$((PASS+1))
  else
    echo "FAIL"
    grep -E "error:|Exception|OutOfMemory" /tmp/sparkforge_out.txt | tail -3 || true
    FAIL=$((FAIL+1))
  fi
}

echo "=== SparkForge 本地测试 ==="
echo

run "[gen]      数据生成"           "runMain com.sparkforge.SparkForgeApp --stage gen      --tuning laptop"
run "[features] 特征管道"           "runMain com.sparkforge.SparkForgeApp --stage features --tuning laptop"
run "[analyze]  小文件分析"         "runMain com.sparkforge.SparkForgeApp --stage analyze"
run "[compact]  文件合并"           "runMain com.sparkforge.SparkForgeApp --stage compact"
run "[case]     nullkey 倾斜"       "runMain com.sparkforge.SparkForgeApp --stage case --case nullkey"
run "[case]     隐式类型转换"       "runMain com.sparkforge.SparkForgeApp --stage case --case cast"
run "[case]     两阶段聚合"         "runMain com.sparkforge.SparkForgeApp --stage case --case twostage"
run "[case]     UDF vs 内置函数"    "runMain com.sparkforge.SparkForgeApp --stage case --case udf"
run "[case]     window 倾斜"        "runMain com.sparkforge.SparkForgeApp --stage case --case window"
run "[advanced] DPP 动态分区裁剪"  "runMain com.sparkforge.SparkForgeApp --stage advanced --demo dpp"
run "[advanced] Bucket Join"        "runMain com.sparkforge.SparkForgeApp --stage advanced --demo bucket"

echo
echo "=== 结果：PASS=$PASS  FAIL=$FAIL ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
