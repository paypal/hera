local _M = {}

local data = {
    [":2004 "] = "1002",
    [":2002 occ "] = "1001 testValue",
    [":2002 occ 1"] = "1001 testValue",
    [",HOST: "] = "5 randomStage_hera-user:load_saved_sessions*CalThreadId=0*TopLevelTxnStartTime=TopLevelTxn not set*Host=randomHost"
}

function _M.get(name)
    return data[name]
end

function _M.get_data()
    return data
end

return _M
