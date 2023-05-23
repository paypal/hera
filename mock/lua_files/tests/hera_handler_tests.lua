local M = {}

function M.main_test()
    package.path = '/usr/local/openresty/nginx/lua_files/tests/?.lua;' .. package.path
    local socket_related_tests = require("socket_related_tests")
    local mock_based_on_data = require("mock_based_on_data")
    local test_utils = require("test_utils")
    local add_mock_test = require("add_mock_test")

    test_utils.set_connect_mock()

    ngx.sleep(5)

    -- test_name is get uri parameter
    -- test_name can be either given as file name such as socket_related_tests
    -- or it can be given as testcase inside a file like close_socket_testcase
    -- if nothing is given or keyword all is given then all the test cases gets executed
    local test_name = test_utils.get_uri_params("name", "all")

    local out_table = {}

    out_table = socket_related_tests.test(test_name, out_table)

    out_table = mock_based_on_data.test(test_name, out_table)

    out_table = add_mock_test.test(test_name, out_table)

    return out_table
end

return M