create or replace function cur_micros
return number
is
    rv number;
    upper number;
begin
    select to_number(to_char(current_timestamp,'SSFF')) into rv from dual;
    select to_number(to_char(current_timestamp,'MI')) into upper from dual;
    rv := rv + 1000000 * 60 * upper;
    -- adding hh24 overflows
    return rv;
end;
/
select cur_micros() from dual;
select cur_micros() as chkStmtSpeed from dual;
create or replace function usleep (micros in number)
return number
is
    finish number;
    cur number;
begin
    cur := cur_micros();
    finish := cur + micros;
    while cur < finish loop
        cur := cur_micros();
    end loop;
    return cur-finish+micros;
end;
/
select current_timestamp from dual;
select usleep(2111000) from dual;
select current_timestamp from dual;
create public synonym usleep for usleep;
--grant execute on usleep to app;

create table resilience_at_load ( id number, note varchar2(333) );
create public synonym resilience_at_load for resilience_at_load;
