#!/bin/bash

if [ -z "$TABLE_NAME" ]; then
	TABLE_NAME=jdbc_mux_test
fi

sed -e "s/jdbc_mux_test/$TABLE_NAME/g" tables.sql | sed -e "s/occ_shard_map/${MGMT_TABLE_PREFIX}occ_shard_map/g" | sed -e "s/occ_whitelist/${MGMT_TABLE_PREFIX}occ_whitelist/g" | $sqlpluscmd $username/$password@"$TWO_TASK" 
sed -e "s/occ_shard_map/${MGMT_TABLE_PREFIX}occ_shard_map/g" shardmap.sql | sed -e "s/occ_whitelist/${MGMT_TABLE_PREFIX}occ_whitelist/g" | $sqlpluscmd $username/$password@"$TWO_TASK"
