local _M = {}

local data = {
    ["FAIL_ON_COMMIT,"] = { ["command"] = "8,", [ "response" ] = "2" },
    ["TIMEOUT_ON_FETCH,"] = { ["command"] = "7 2,", [ "response" ] = "timeout" },
    ["FAIL_ON_ROLLBACK,"] = { ["command"] = "9,", [ "response" ] = "2" },
    ["FAIL_SET_SHAREDID,"] = { ["command"] = "29 ", [ "response" ] = "2" },
    ["FAIL_ON_PING,"] = { ["command"] = "1008,", [ "response" ] = "1007" },
    ["TIMEOUT_ON_COMMIT,"] = { ["command"] = "8,", [ "response" ] = "timeout" },
    ["TIMEOUT_ON_ROLLBACK,"] = { ["command"] = "9,", [ "response" ] = "timeout" },
    ["PROTOCOL_ERROR_ON_COMMIT,"] = { ["command"] = "8,", ["response"] = "0"},
    ["PROTOCOL_EXTRA_DATA_ON_ROLLBACK,"] = { ["command"] = "9,", ["response"] = "5, NEXT_NEWSTRING 5"},
    ["PROTOCOL_ERROR_ON_ROLLBACK,"] = { ["command"] = "9,", ["response"] = "0"}
}

function _M.get(name)
    return data[name]
end

function _M.get_command(name)
    return data[name]["command"]
end

function _M.get_response(name)
    return data[name]["response"]
end

return _M
