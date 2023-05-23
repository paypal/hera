local M = {}

function M.find_last(from_string, find_string)
    --Set the third arg to false to allow pattern matching
    local found = from_string:reverse():find(find_string:reverse(), nil, true)
    if found then
        return from_string:len() - find_string:len() - found + 2
    else
        return found
    end
end

function M.split(string_to_split, delimiter)
    local result = {};
    for match in (string_to_split..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

function M.starts_with(source_string, search_string)
    return source_string:sub(1, #search_string) == search_string
end

function M.escape_special_chars(inp)
    local resp = inp
    local special_char = {
        ["heraMockEqual"] = "=",
        ["heraMockUnaryAnd"] = "&",
        ["heraMockPlus"] = "+"
    }
    for k,v in pairs(special_char)
    do
        resp = resp:gsub(k, v)
    end
    return resp
end

return M