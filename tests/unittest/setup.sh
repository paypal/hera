#!/bin/bash

if [ -z "$TABLE_NAME" ]; then
	TABLE_NAME=jdbc_hera_test
fi

if [ -z "$MGMT_TABLE_PREFIX" ]; then
	MGMT_TABLE_PREFIX=hera
fi

sed -e "s/jdbc_hera_test/$TABLE_NAME/g" tables.sql | sed -e "s/hera_shard_map/${MGMT_TABLE_PREFIX}_shard_map/g" | sed -e "s/hera_whitelist/${MGMT_TABLE_PREFIX}_whitelist/g"  | sed -e "s/hera_maint/${MGMT_TABLE_PREFIX}_maint/g" | $sqlpluscmd $username/$password@"$TWO_TASK" 
sed -e "s/hera_shard_map/${MGMT_TABLE_PREFIX}_shard_map/g" shardmap.sql | sed -e "s/hera_whitelist/${MGMT_TABLE_PREFIX}_whitelist/g" | $sqlpluscmd $username/$password@"$TWO_TASK"
