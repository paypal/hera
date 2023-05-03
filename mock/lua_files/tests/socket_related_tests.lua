local M = {}

function M.close_socket_testcase()
    --
    -- this testcase is to verify if the mock is closing a given socket as requested
    -- in this case mock is instructed to close the socket once it receives query with name AcctMap._PrimaryKeyLookup
    -- note in case of real time use case there will be one retry by driver layer. So mock is set to expect that
    --

    -- setting mock
    local test_utils = require("test_utils")

    local value = "CLOSE_SOCKET, NEXT_NEWSTRING NEXT_COMMAND_REPLY  CLOSE_SOCKET"
    test_utils.add_mock("Emp.Query", value)

    for _ = 2, 1, -1
    do
        test_utils.log_to_file(ngx.DEBUG, "make connection")
        local status, up_sock, sock_id = test_utils.make_connection()
        if not status then
            test_utils.log_to_file(ngx.DEBUG, "connection failed")
            return "FAILED"
        end
        test_utils.log_to_file(ngx.DEBUG, "ready request")
        if status then
            local request = "60:0 39:25 select * /*Emp.Query*/ from employee,1:4,2:22,3:7 2,,"
            test_utils.log_to_file(ngx.DEBUG, "sending " .. request)
            status, value = test_utils.send_to_server(up_sock, request, sock_id)
            if not status then
                return "FAILED"
            end
            test_utils.log_to_file(ngx.DEBUG, "reading... " )
            status, value = test_utils.read_from_server(up_sock, sock_id)
            if status then
                return "FAILED"
            end
        end
    end
    return "PASSED"
end

function M.test(name, in_table)
    local out_table = {}
    local test = "socket_related_tests"
    if name ~= "all" and M[name] ~= nil then
        out_table[test .. "." .. name] = M[name]()
    elseif name == "all" or name == test then
        out_table[test .. ".close_socket_testcase"] = M.close_socket_testcase()
    end
    local test_utils = require("test_utils")
    return test_utils.merge_tables(out_table, in_table)
end

return M