drop table jdbc_hera_test;

create table jdbc_hera_test (
ID                             NUMBER,
INT_VAL                        NUMBER,
STR_VAL                        VARCHAR2(500)
);

drop table hera_shard_map;

create table hera_shard_map (
SCUTTLE_ID                     NUMBER,
SHARD_ID                       NUMBER,
STATUS                         CHAR(1),
READ_STATUS                    CHAR(1),
WRITE_STATUS                   CHAR(1) ,
REMARKS                        VARCHAR2(500)
);

drop table hera_whitelist;

create table hera_whitelist (
SHARD_KEY                      NUMBER NOT NULL,
SHARD_ID                       NUMBER NOT NULL,
ENABLE                         CHAR(1) NOT NULL,
READ_STATUS                    CHAR(1) NOT NULL,                                                                                                                                                                       
WRITE_STATUS                   CHAR(1) NOT NULL,
REMARKS                        VARCHAR2(500) NOT NULL
); 

drop table hera_maint;

create table hera_maint (
INST_ID NUMBER, 
MACHINE VARCHAR(512), 
STATUS VARCHAR(8), 
STATUS_TIME NUMBER, 
MODULE VARCHAR(64)
);
 
