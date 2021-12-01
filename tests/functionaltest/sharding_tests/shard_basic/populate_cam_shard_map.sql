/********setup for sharding**********/
use heratestdb;
delete from hera_shard_map;
call populate_shard_map(128);
 
quit
