-- use this file as a initialization for your mysql. you can create your schema, tables, permission etc

-- example is given below

create table employee
(
    TIME_CREATED timestamp    null,
    VERSION      int(10)      null,
    ID           int(10) auto_increment
        primary key,
    NAME         varchar(100) null
);

insert into employee(time_created, version, name) values (now(), 1, 'Hera');