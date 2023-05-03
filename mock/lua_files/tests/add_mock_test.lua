local M = {}
package.path = '/usr/local/openresty/nginx/lua_files/tests/?.lua;' .. package.path
package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path
local test_utils = require("test_utils")

function M.simple_add_mock()
    test_utils.add_mock("key", "value")
    local data = test_utils.list_mock()
    local expect = "NEXT_LINE key=value NEXT_LINE"
    if string.find(data, expect) then
        return "PASSED"
    end
    return "FAILED"
end

function M.test(name, in_table)
    local out_table = {}
    local test = "add_mock_tests"
    if name ~= "all" and M[name] ~= nil then
        out_table[test .. "." .. name] = M[name]()
    elseif name == "all" or name == test then
        out_table[test .. ".simple_add_mock"] = M.simple_add_mock()
    end
    return test_utils.merge_tables(out_table, in_table)
end

return M