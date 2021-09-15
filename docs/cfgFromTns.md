Automatic Configuration from tnsnames.ora
-----------------------------------------

We use conventions in tnsnames.ora to help derive Hera mux configuration.
A hera service hera-winky uses a TWO_TASK=WINKY for the basic case. 

For read-write split in low latency DB clusters, we set 
TWO_TASK_READ=WINKY_HERA .

For hera-winky_r1 (TWO_TASK=WINKY_R1), adding a read-replica failover 
(transparent application failover, TAF), we set TWO_TASK_STANDBY0=WINKY_R2 .

For sharding, we set TWO_TASK_0=WINKY_SH0 , TWO_TASK_1=WINKY_SH1 .. and
optionally TWO_TASK_READ_0=WINKY_HERA_SH0 , TWO_TASK_READ_1=WINKY_HERA_SH1 

If we sharded hera-winky_r1, we would have TWO_TASK_0=WINKY_R1_SH0 , 
TWO_TASK_1=WINKY_R1_SH1 ..and optionally TWO_TASK_STANDBY0_0=WINKY_R2_SH0 , 
TWO_TASK_STANDBY0_1=WINKY_R2_SH1 ..

Overrides
=========

If the conventions aren't followed, disable in hera.txt cfg_from_tns=false

If the number of shards aren't correct, cfg_from_tns_override_num_shards=3

To disable TAF, cfg_from_tns_override_taf=0 . To enable TAF,
cfg_from_tns_override_taf=1 . The default is -1, telling the code to 
configure automatically..

If read-write split isn't detected correctly, 
cfg_from_tns_override_rw_split=40 to allocate 40% max connection configuration
for read workers.
