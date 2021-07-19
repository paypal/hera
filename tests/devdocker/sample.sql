create database testschema;
use testschema;
create table sample ( rstatus char(1), wstatus char(1) );
insert into sample (rstatus, wstatus) values ('F', 'E');
insert into sample (rstatus, wstatus) values ('E', 'F');
