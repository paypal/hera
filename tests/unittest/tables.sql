drop table jdbc_mux_test;

create table jdbc_mux_test (
ID                             NUMBER,
INT_VAL                        NUMBER,
STR_VAL                        VARCHAR2(500)
);

drop table occ_shard_map;

create table occ_shard_map (
SCUTTLE_ID                     NUMBER,
SHARD_ID                       NUMBER,
STATUS                         CHAR(1),
READ_STATUS                    CHAR(1),
WRITE_STATUS                   CHAR(1) ,
REMARKS                        VARCHAR2(500)
);

drop table occ_whitelist;

create table occ_whitelist (
SHARD_KEY                      NUMBER NOT NULL,
SHARD_ID                       NUMBER NOT NULL,
ENABLE                         CHAR(1) NOT NULL,
READ_STATUS                    CHAR(1) NOT NULL,                                                                                                                                                                       
WRITE_STATUS                   CHAR(1) NOT NULL,
REMARKS                        VARCHAR2(500) NOT NULL
); 
