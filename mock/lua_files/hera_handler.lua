local function log_to_file(level, data)
	if ((ngx.shared.mock_response:get("DISABLE_LOG") == nil and ngx.shared.mock_response:get("DISABLE_FILE_LOG") == nil)
	 or (level == ngx.ERR)) then
		local logdata = debug.getinfo(2).currentline .. ':' .. data
		while(#logdata > 0) do
			local temp = ngx.var.server_port .. ':' .. string.sub(logdata, 0, 4000)
			ngx.log(level, temp)
			logdata = string.sub(logdata, 4001)
		end
	end
end

local function run_capture(cmd)
	local f = assert(io.popen(cmd, 'r'))
	local s = assert(f:read('*a'))
	f:close()
	return s
end

local function split(s, delimiter)
	local result = {};
	for match in (s..delimiter):gmatch("(.-)"..delimiter) do
		table.insert(result, match);
	end
	return result;
end

local function ignore_regex_split (input_str, sep)
	if sep == nil then
		sep = "%s"
	end
	local t={}
	for str in string.gmatch(input_str, "([^"..sep.."]+)") do
		table.insert(t, str)
	end
	return t
end

local function collect_response_for_redis(log_id, sock, red, data)
	local key = ngx.shared.redis_req_res:get(sock)
	local edata = ngx.shared.redis_response:get(key)
	local new_data = data
	if(edata ~= nil) then
		new_data = edata .. " NEXT_NEWSTRING " .. data
	end
	local status, error, _ = ngx.shared.redis_response:set(key, new_data, 600)
	if (error ~= nil and status == false) then
	    log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
	end
end

local function get_sql_hash(log_id, q)
	local sql_hash = ngx.shared.sql_hash_cache:get(q)
	if (sql_hash == nil or sql_hash == '') then
		local handler = assert(io.popen("cd /opt && echo `/usr/bin/java SQLHashGen \""..q.."\"` | tr -d '\n'"))
		sql_hash = handler:read("*a")
		local _, error, _ = ngx.shared.sql_hash_cache:set(q, sql_hash)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		handler:close()
	end
	return sql_hash
end

local function log_request_to_redis(log_id, data)
	local key
	if (ngx.shared.mock_response:get("DISABLE_LOG") == nil) then
		if (string.find(data, ":25 ")) then
			local d = split(data, ":25 ")
			local x = split(d[1], ",")
			x = split(x[#x], " ")
			local l = tonumber(x[#x])-3
			local q = d[2]:sub(1, l)
			local corr_id = "NotSet"
			if (string.find(data, ":2006 ")) then
				local c = split(split(data, ":2006 ")[2], ",")[1]
				if (string.find(c, "=")) then
					corr_id = split(c, "=")[2]
				end
				if (string.find(corr_id, "&")) then
					corr_id = split(corr_id, "&")[1]
				end
			end

			local sql_hash = get_sql_hash(log_id, q)

			local t = tostring(ngx.var.msec)
			key =   t .. ":" .. corr_id .. ":" .. sql_hash .. ":" .. q

			local req_data = data .. " START_RESPONSE "
			local _, error, _ = ngx.shared.redis_response:set(key, req_data, 600)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
		elseif string.find(data, "%d,$") then
			local req_data = data .. " START_RESPONSE "
			local t = tostring(ngx.var.msec)
			key =   t .. ":NA:NA:Command"
			local _, error, _ = ngx.shared.redis_response:set(key, req_data, 600)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
		end
	end
	return key
end

local function read_net_string_data(socket_obj, socket_obj_name, log_id)
	local delimiter = ":"
	if  socket_obj == nil then
		return 0, nil
	end
	local readline = socket_obj:receiveuntil(delimiter)
	local size, _, _ = readline()
	if(not size) then
		log_to_file(ngx.DEBUG, log_id .. " no data recv to " .. socket_obj_name)
		return nil, nil
	end
	local data = socket_obj:receive( tonumber(size) + 1)
	return size, data
end

local function send_net_string_data(socket_obj, socket_obj_name, data_size, data, log_id)
	local data_to_send = data_size .. ":" .. data
	if socket_obj == "mocked" then
		log_to_file(ngx.ERR, log_id .. " ERR - Should not have reached here \
			server connect was mocked so not sending " .. data_to_send)
		return 'ok', nil
	end
 	local _, err = socket_obj:send(data_to_send)
	if err then
		log_to_file(ngx.ERR, log_id .. " no data send to " .. socket_obj_name .. " err: " .. err)
		local _, error, _ = ngx.shared.response_status:set(ngx.shared.mock_connection_corr:get(), "failed: " .. data_to_send, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		return nil, 'err'
	else
		return 'ok', nil
	end

end

local function isProperIP(ip)
	-- must pass in a string value
	if ip == nil or type(ip) ~= "string" then
		return false
	end

	-- check for format 1.11.111.111 for ipv4
	local chunks = {ip:match("(%d+)%.(%d+)%.(%d+)%.(%d+)")}
	if (#chunks == 4) then
		for _,v in pairs(chunks) do
			if (tonumber(v) < 0 or tonumber(v) > 255) then
				return false
			end
		end
		return true
	else
		return false
	end

	-- check for ipv6 format, should be 8 'chunks' of numbers/letters
	local _, chunks = ip:gsub("[%a%d]+%:?", "")
	if chunks == 8 then
		return true
	end

	-- if we get here, assume we've been given a random string
	return false
end

local function trim(s)
	return (s:gsub("^%s*(.-)%s*$", "%1"))
end

local function add_comma_at_end(response_mock_data)
	if response_mock_data ~= nil and string.len(response_mock_data) > 0 then
		if string.find(response_mock_data, ',', -1) == nil then
			return response_mock_data .. ','
		end
	end
	return response_mock_data
end

local function get_id(o)
	if type(o) == 'table' then
		for x, v in pairs(o) do
			local resp = get_id(v)
			if resp ~= nil and resp ~= "unknown" then
				return string.gsub(resp, "%s+", "")
			end
		end
	else
		if tostring(o) == "mocked" then
			return "mocked"
		elseif string.find(tostring(o), "userdata") then
			local resp = split(tostring(o), ":")
			return string.gsub(resp[2], "%s+", "") .. "_" .. ngx.var.connection .. "_" .. ngx.var.remote_port
		end
	end
	return "unknown"
end


local function is_connect_mocked()
	local upstream_ip = ngx.shared.mock_response:get("DALHERAMOCK_SERVERIP")
	if upstream_ip ~= nil and upstream_ip == "MOCK_SERVER" then
		return true
	end

	local port_specific_ip = ngx.shared.mock_response:get("PORT_DALHERAMOCK_SERVERIP")
	if port_specific_ip ~= nil and port_specific_ip ~= "" then
		local val = port_specific_ip
		if string.find(port_specific_ip, ":") then
			local ip_ports = split(port_specific_ip, ":")
			val = ip_ports[1]
		end
		if  val == "MOCK_SERVER" then
			return true
		end
	end
    return false
end

local function get_upstream_socket()
	if is_connect_mocked() then
		return "mocked", "mocked"
	end

	local up_sock = assert(ngx.socket.tcp())
	local port = ngx.var.server_port
	local temp_id = get_id(up_sock)
	local backed_by = ngx.shared.mock_response:get("DALHERAMOCK_SERVERIP")
	local port_specific_ip = ngx.shared.mock_response:get("PORT_DALHERAMOCK_SERVERIP")

    if ngx.shared.port_map:get(port) ~= nil then
        port = ngx.shared.port_map:get(port)
    end

	if os.getenv("BACKEDBY_ENV_NAME") ~= nil then
		backed_by = os.getenv("BACKEDBY_ENV_NAME")
	end

	if port_specific_ip ~= nil and port_specific_ip ~= "" and string.find(port_specific_ip, ":") then
		local ip_ports = split(port_specific_ip, ":")
		for server_port in (ip_ports[2]..","):gmatch("(.-)"..",") do
			if server_port == port then
				backed_by = ip_ports[1]
				break
			end
		end
	end

	local upstream_ip = backed_by
	if not isProperIP(backed_by) then
		upstream_ip = run_capture("nslookup " .. backed_by .. " | grep \"Address\"| tail -1 | cut -d' ' -f2 | head -c -1")
	end

	local up_ok, up_err = up_sock:connect(upstream_ip, port)
	if not up_ok then
		log_to_file(ngx.ERR, temp_id .. " upstream connection failure " .. up_err .. " for ip " .. upstream_ip .. ":" .. port)
		return nil, upstream_ip
	else
		temp_id = get_id(up_sock)
	end
	local _, up_err = up_sock:sslhandshake()
	if up_err then
		log_to_file(ngx.ERR, temp_id .. " upstream handshake failed " .. up_err .. " for ip " .. upstream_ip .. ":" .. port)
		return nil, upstream_ip
	end
	log_to_file(ngx.DEBUG, temp_id .. " new upstream socket connection" .. " for ip " .. upstream_ip .. ":" .. port)
	return up_sock, upstream_ip
end

local function check_for_commands(client_data, m_data, log_id, sock_id)
	local cm = ngx.shared.mock_connection:get(sock_id)
	if cm then
		log_to_file(ngx.DEBUG, log_id .. " finding mock resp for ".. sock_id .. " to " .. cm)
		local a = require("mock_constant_response")
		if a.get(cm) then
			if string.find(client_data, a.get_command(cm)) == 1 then
				m_data = a.get_response(cm)
				log_to_file(ngx.DEBUG, log_id .. " deleting mock for  ".. sock_id)
				ngx.shared.mock_connection:delete(sock_id)
				local key = ngx.shared.mock_connection_corr:get(sock_id)
				if key ~= nil then
					log_to_file(ngx.DEBUG, log_id .. " deleting mock for  ".. key)
					ngx.shared.mock_response:delete(key)
				end
			else
				m_data = ""
			end
			log_to_file(ngx.DEBUG, log_id .. " found ".. m_data)
		end
	end
	return m_data
end

local function ends_with(str, ending)
	return ending == "" or str:sub(-#ending) == ending
end

local function starts_with(str, start)
	return str:sub(1, #start) == start
end

local function check_and_capture_response(sock, resp, log_id)
	if ngx.shared.capture_key:get(sock) then
		local corr_id = ngx.shared.capture_key:get(sock)
	    if string.find(corr_id, "CAPTURE_SQL,") then
			if ngx.shared.capture_req_resp:get(corr_id) ~= nil then
				local uresp = ngx.shared.capture_req_resp:get(corr_id)
				if not ends_with(uresp, " START_RESPONSE ") then
					uresp = uresp .. " NEXT_NEWSTRING "
				end
				uresp = uresp .. resp
				local _, error, _ = ngx.shared.capture_req_resp:set(corr_id, uresp, 600)
				if (error ~= nil ) then
                    log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                end
			end
		else
			log_to_file(ngx.DEBUG, log_id .. " Capturing resp for: " .. corr_id)
			local key = ngx.shared.current_capture_corr:get(sock .. "_LATEST")
			if ngx.shared.capture_req_resp:get(corr_id) ~= nil then
				local uresp = ngx.shared.capture_req_resp:get(key)
				if not ends_with(uresp, " START_RESPONSE ") then
					uresp = uresp .. " NEXT_NEWSTRING "
				end
				uresp = uresp .. resp
			    if ngx.shared.current_capture_corr:get(sock .. "_INLINE_CAPTURE") ~= nil then
					log_to_file(ngx.DEBUG, log_id .. " new inline mock for  ".. key .. ": " .. uresp)
					local _, error, _ = ngx.shared.mock_response:set(key, uresp, ngx.shared.current_capture_corr:get(sock .. "_INLINE_CAPTURE"), 600)
					if (error ~= nil ) then
                        log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                    end
				end
				local _, error, _ = ngx.shared.capture_req_resp:set(key, uresp, 600)
				if (error ~= nil ) then
                    log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                end
			end
		end
		return true
	end
	return false
end

local function clean_up_capture_data(r_sock, sock_id)
	if ngx.shared.capture_key:get(r_sock) then
		ngx.shared.capture_key:delete(r_sock)
	end
	if ngx.shared.current_capture_corr:get(r_sock .. "_INLINE_CAPTURE") then
		ngx.shared.current_capture_corr:delete(r_sock .. "_INLINE_CAPTURE")
	end
	if  ngx.shared.mock_connection:get(sock_id) and
			(ngx.shared.mock_connection:get(sock_id) == "CAPTURE," or
					ngx.shared.mock_connection:get(sock_id) == "CAPTURE_SQL,")then
		ngx.shared.mock_connection:delete(sock_id)
		ngx.shared.mock_corr_connection:delete(sock_id)
	end
end

local function trim_request(client_data)
	local trim_data = client_data
	if  string.find(client_data, ":25 ") then
		local qs = split(client_data, ":25 ")
		local td = split(qs[1], ",")
		local lend = td[#td]
		trim_data = lend .. ":25 "
		for i=2,#qs do trim_data = trim_data .. qs[i] end
	end
	return trim_data
end


local function check_and_capture_request(key, response_mock_data, r_sock, client_data, log_id, sock_id, ttl)
	-- if capture enabled during this call
	if response_mock_data == "CAPTURE," or response_mock_data == "INLINE_CAPTURE" or
			-- or capture was already enabled in the connection level
			((key == nil or key == "") and ngx.shared.mock_connection:get(sock_id) and
					ngx.shared.mock_connection:get(sock_id) == "CAPTURE," and client_data ~= "1008 ,") then

		if (key == nil or key == "") then
			if not ngx.shared.mock_corr_connection:get(sock_id) then
				return false
			end
			key = ngx.shared.mock_corr_connection:get(sock_id)
		end

		local raw_key = key
		if response_mock_data ~= "INLINE_CAPTURE" then
			key = key .. "_" .. sock_id
		end

        -- hint to capture response
		local _, error, _ = ngx.shared.capture_key:set(r_sock, key, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		local capture_value = "NEW_REQUEST "
		local new_val, _, _ = ngx.shared.capture_order_counter:incr(raw_key, 1, 0)
		_, error,_ = ngx.shared.capture_order:set(raw_key .. ':' .. new_val, key, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end

		if ngx.shared.capture_req_resp:get(key) ~= nil and response_mock_data ~= "INLINE_CAPTURE" then
			capture_value = ngx.shared.capture_req_resp:get(key) .. " " .. capture_value
		end


		capture_value = capture_value .. trim_request(client_data) .. " START_RESPONSE "
		_, error,_ = ngx.shared.current_capture_corr:set(r_sock .. "_LATEST", key, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		_, error,_ = ngx.shared.capture_req_resp:set(key, capture_value, 600)
        if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		_, error,_ = ngx.shared.mock_connection:set(sock_id, "CAPTURE,", 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		_, error,_ = ngx.shared.mock_corr_connection:set(sock_id, raw_key, 600)
        if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		if response_mock_data == "INLINE_CAPTURE" then
			_, error,_ = ngx.shared.current_capture_corr:set(r_sock .. "_INLINE_CAPTURE", ttl)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
		end

		log_to_file(ngx.DEBUG, log_id .. " REQUEST_CAPTURE " .. key .. " " .. new_val)
		log_to_file(ngx.DEBUG, log_id .. " setting connection capture mock " .. sock_id .. ": " .. raw_key .. ":" .. r_sock)
		return true
	elseif response_mock_data == "CAPTURE_SQL," then
		_, error,_ = ngx.shared.capture_key:set(r_sock, response_mock_data .. key, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		_, error,_ = ngx.shared.capture_req_resp:set(response_mock_data .. key,
			client_data .. " START_RESPONSE ", 600)
        if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
		return true;
	else
		clean_up_capture_data(r_sock, sock_id)
		return false
	end
end

local function get_comment(sql)
	local comment = string.match(sql, "(.-)*/")
	if comment ~= nil then
		comment = trim(string.sub(comment, string.find(comment, "%/%*")+2))
	end
	return comment
end

local function check_replay_mode(client_data, multi_keys, log_id)
	local res_delimiter = " START_RESPONSE "
	local req_delimiter = "NEW_REQUEST "
	local response = "REPLAY_MODE_RESPONSE,"
	local key_size = 0
	local ttl = 0
	local err
	local first_name_match_resp
	local key_to_delete

	for key in (multi_keys..","):gmatch("(.-)"..",") do
		if key ~= "" then
			key_size = key_size + 1
			ttl, err = ngx.shared.mock_response:ttl(key)
			local response_mock_data = ngx.shared.mock_response:get(key)
            if response_mock_data ~= nil then
                local mock_request = split(split(response_mock_data, res_delimiter)[1], req_delimiter)[2]
                local mock_res = split(response_mock_data, res_delimiter)[2]
                local current_req_query = trim_request(client_data)

                if mock_request == current_req_query then
                    ngx.shared.mock_response:delete(key)
                    response = mock_res
                    log_to_file(ngx.DEBUG, log_id .. " replay query exact match " .. key)
                elseif first_name_match_resp == nil then
                    if string.find(current_req_query, "/[*]") and string.find(current_req_query, "[*]/") and
                            string.find(mock_request, "/[*]") and string.find(mock_request, "[*]/") then
                        local cq_name = ignore_regex_split(ignore_regex_split(current_req_query, "/*")[2], "*/")[1]
                        local mr_name = ignore_regex_split(ignore_regex_split(mock_request, "/*")[2], "*/")[1]
                        log_to_file(ngx.DEBUG, log_id .. " cq_name " .. cq_name .. " mr_name " .. mr_name)
                        if cq_name == mr_name then
                            first_name_match_resp = mock_res;
                            key_to_delete = key
                            log_to_file(ngx.DEBUG, log_id .. " replay name match " .. key_to_delete ..
                                    " first_name_match_resp " .. first_name_match_resp)
                            if string.find(mock_request, ",2:22,") and
                                    string.find(current_req_query, ",2:22,") == nil then
                                local delimiter = " NEXT_NEWSTRING "
                                first_name_match_resp = ""
                                local count_rep_line = 0
                                for line in (mock_res..delimiter):gmatch("(.-)"..delimiter) do
                                    if count_rep_line ~= 1 then
                                        first_name_match_resp = first_name_match_resp .. delimiter .. line
                                    end
                                    count_rep_line = count_rep_line + 1
                                end
                                break
                            end
                        end
                    end
                end
                if response ~= "REPLAY_MODE_RESPONSE," then
                    break
                end
            end
		end
	end
	if response == "REPLAY_MODE_RESPONSE," and first_name_match_resp ~= nil then
		response = first_name_match_resp
		ngx.shared.mock_response:delete(key_to_delete)
	end

	return response, ttl, key_size
end

local function check_for_connection_replay(sock_id, log_id)
	if not ngx.shared.mock_corr_connection:get(sock_id) then
		return ""
	end
	log_to_file(ngx.DEBUG, log_id .. " connection level mock socket " .. sock_id)
	local key = ngx.shared.mock_corr_connection:get(sock_id)
	local resp = ngx.shared.mock_response:get(key)
	local rmd = ""
	if resp and string.len(resp) > 0 then
		rmd = add_comma_at_end(resp)
		log_to_file(ngx.DEBUG, log_id .. " connection level mock " .. key .. ": " .. rmd)
		if not rmd then
			rmd = ""
		end
	end
	return rmd
end

local function check_for_connection_mock(log_id, client_data)
    -- check for connection level mock for given client ip
    if is_connect_mocked() then
        local a = require("connection_mock_constant")
        local keys =  a:get_data();
		for key,value in pairs(keys)
		do
            if string.find(client_data, key) then
				log_to_file(ngx.DEBUG, log_id .. " got mock data for connect " .. a.get(key))
                return a.get(key)
            elseif starts_with(client_data, "0 ") and string.find(key, client_data) then
                log_to_file(ngx.DEBUG, log_id .. " got mock data for connect " .. a.get(key))
                return a.get(key)
            end
		end
    end
    return ""
end

local function shuffle(tbl)
	for i = #tbl, 2, -1 do
		local j = math.random(i)
		tbl[i], tbl[j] = tbl[j], tbl[i]
	end
	return tbl
end

local function generate_next_failure_set(log_id, name, count, sampling)

	local nums = {}
	for i=1, sampling do
		nums[i] = i
		local _, error,_ = ngx.shared.random_response_store:set("name_" .. i, 3, 600)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
	end

	local p = count
	if(sampling < 100) then
		p = (count*sampling)/100
	end

	nums = shuffle(nums)

	for i = 1, p do
	    local _, error,_ = ngx.shared.random_response_store:set("name_" .. nums[i], 4, 600)
        if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
	end
	log_to_file(ngx.DEBUG, log_id .. " percentage " .. count .. " name " .. name)
end

local function get_random_resp(sql_name, resp, log_id)
	local cdata = split(resp, " MKEYSEP ")
	local cnt = ngx.shared.random_failure_state:get(sql_name)

	local percentage = cdata[1]
    local sampling = cdata[2]

	if cnt == nil or cnt == 1 or cnt >= 101 then
		cnt = 1
		generate_next_failure_set(log_id, sql_name, tonumber(percentage), tonumber(sampling))
	end

	log_to_file(ngx.DEBUG, log_id .. sql_name .. ":" .. cnt)

	local _, error,_ = ngx.shared.random_failure_state:set(sql_name, cnt+1, 600)
    if (error ~= nil ) then
        log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
    end
	return cdata[ngx.shared.random_response_store:get("name_" .. cnt)]
end

local function get_query(data)
	local d = split(data, ":25 ")
	local x = split(d[1], ",")
	x = split(x[#x], " ")
	local l = tonumber(x[#x])-3
	return d[2]:sub(1, l)
end

local function get_object_mock_rows(mock_data, data, key, log_id, forever_mock)
	local query = get_query(data)
	if(forever_mock) then
		local new_mock_data = ngx.shared.object_mock_cache:get(key)
		if(new_mock_data ~= nil) then
			return new_mock_data;
		end
	end
	log_to_file(ngx.DEBUG, log_id .. " cache not found for object mock ")
	local j_cmd = "/usr/bin/java -jar /opt/heramockclient-jar-with-dependencies.jar"
	local params = "\"QUERY_RESPONSE\" \"" .. query .. "\" \"" ..  mock_data .. "\""
	local cmd = "echo `" .. j_cmd .. " " .. params .. "` | tr -d '\n'"
	local handler = assert(io.popen(cmd))
	local new_mock_data = handler:read("*a")
	handler:close()
	if(forever_mock) then
		local ttl, _ = ngx.shared.mock_response:ttl(key)
		local _, error,_ = ngx.shared.object_mock_cache:set(key, new_mock_data, ttl)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
	end
    return new_mock_data;
end

local function get_object_mock_meta(mock_data, data, key, log_id, forever_mock)
	local query = get_query(data)
    if(forever_mock) then
        local new_mock_data = ngx.shared.object_mock_cache_meta:get(key)
        if(new_mock_data ~= nil) then
            return new_mock_data;
        end
    end
	log_to_file(ngx.DEBUG, log_id .. " parsing query and getting mock response ")
	local j_cmd = "/usr/bin/java -jar /opt/heramockclient-jar-with-dependencies.jar"
	local params = "\"QUERY_META\" \"" .. query .. "\" \"" ..  mock_data .. "\""
	local cmd = "echo `" .. j_cmd .. " " .. params .. "` | tr -d '\n'"
	log_to_file(ngx.DEBUG, log_id ..  cmd)
	local handler = assert(io.popen(cmd))
	local new_mock_data = handler:read("*a")
	handler:close()
    if(forever_mock) then
        local ttl, _ = ngx.shared.mock_response:ttl(key)
        local _, error,_ = ngx.shared.object_mock_cache_meta:set(key, new_mock_data, ttl)
        if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
        return new_mock_data;
    end
	return new_mock_data;
end

local function check_object_mock_trim_meta_data(mock_data, client_data, log_id, key, forever_mock)
	local new_mock_data = mock_data
	if string.find(mock_data, "HERAMOCK_OBJECT_MOCK_META ") then
		log_to_file(ngx.DEBUG, log_id .. " found object mock ")
		-- if requested for column details, just remove HERAMOCK_OBJECT_MOCK_META else remove entire col details
		if string.find(client_data, ",2:22,") then
			new_mock_data = get_object_mock_meta(mock_data, client_data, key, log_id, forever_mock)
			log_to_file(ngx.DEBUG, log_id .. " queried with meta so just trimming keyword " .. new_mock_data)
		else
			new_mock_data = get_object_mock_rows(mock_data, client_data, key, log_id, forever_mock)
			log_to_file(ngx.DEBUG, log_id .. " queried without meta - so just trimming meta " .. new_mock_data)
		end
	end
	return new_mock_data;
end

local function get_multikey(log_id, corr_id, sock_id)
	local multi_keys = ""
	local keys =  ngx.shared.mock_response:get_keys();
	for x, value in pairs(keys) do
		if string.find(value, "_") and (split(value, "_")[1] == corr_id or
				split(value, "_")[1] == corr_id:sub(0, 3)) then
			local _, error,_ = ngx.shared.mock_corr_connection:set(sock_id, corr_id, 600)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
			_, error,_ = ngx.shared.mock_response:set(corr_id, "REPLAY_MODE_RESPONSE,", 120)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
			multi_keys = multi_keys .. ',' .. value
		end
	end
	return multi_keys
end

local function get_mock_data(client_data, log_id, sock, r_sock)
	local response_mock_data = ""
	local key = ""
    local multi_keys = ""
	local sock_id = get_id(sock)
    local corr_id = ""
	local match_3_char_corr_id
	local forever_mock = false

	local captured_response = check_and_capture_response(sock_id, client_data, log_id)

	-- if data sent from client to server has correlation id - grab it into variable corr_id
	-- and check if the corr_id has a key in mock_response, if so we have mock for this request
	if (string.find(client_data, ":2006 ") and string.find(client_data, "CorrId=")) then
		-- 2006 is hera netstring protocol to send correlation id
		local s, e = string.find(client_data, ":2006 ")
		local c = client_data:sub(e+1)

		for word in string.gmatch(c, '([^&]+)') do
			s, e = string.find(word, "CorrId=")
			if e then
				corr_id = word:sub(e+1)
				s, e = string.find(corr_id, ",")
				if e then
					corr_id = corr_id:sub(0, e-1)
				end
				s, e = string.find(corr_id, "&")
				if e then
					corr_id = corr_id:sub(0, e-1)
				end

				if corr_id ~= "NotSet" and trim(corr_id) ~= "" then
					log_to_file(ngx.DEBUG, log_id .. " Looking for corr_id: " .. corr_id)
				end
				-- if we get a new correlation id store it in our socket to corr_id map
				local lc = ngx.shared.mock_connection_corr:get(sock_id)
				if lc and lc ~= corr_id then
					log_to_file(ngx.DEBUG, log_id .. " deleting previous mock for " .. lc .. " for socket " .. sock_id)
					ngx.shared.mock_connection:delete(sock_id)
				end

				-- check if we have defined a mock for this corrid
				local resp = ""
				if ngx.shared.mock_response:get(corr_id) and
						ngx.shared.mock_response:get(corr_id) ~= "REPLAY_MODE_RESPONSE," then
					resp = ngx.shared.mock_response:get(corr_id)
					key = corr_id
				else
					multi_keys = get_multikey(log_id, corr_id, sock_id)
					if multi_keys == "" then
						-- check if first three characters of mock matches
						match_3_char_corr_id = corr_id:sub(0, 3)
						resp = ngx.shared.mock_response:get(match_3_char_corr_id)
						key = corr_id:sub(0,3)
					else
						key = corr_id
						resp = "REPLAY_MODE_RESPONSE"
					end
				end
				if resp and string.len(resp) > 0 then
					response_mock_data = add_comma_at_end(resp)
					if not response_mock_data then
						response_mock_data = ""
					end
				end
			end
		end
	end
	if corr_id ~= "" then
		local _, error,_ = ngx.shared.mock_connection_corr:set(sock_id, corr_id, 120)
		if (error ~= nil ) then
            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
        end
	else
		local sockets_corr_id = ngx.shared.mock_corr_connection:get(sock_id)
		if ngx.shared.mock_response:get(sockets_corr_id) == "REPLAY_MODE_RESPONSE," then
			corr_id = sockets_corr_id
			multi_keys = get_multikey(log_id, corr_id, sock_id)
		end
	end

    if (string.len(response_mock_data) <=0) then
        -- this one mocks the connection itself. If so we dont need server at all
        response_mock_data = check_for_connection_mock(log_id, client_data)
    end

	if (string.len(response_mock_data) <=0) then
		if (string.find(client_data, ":2002 occ 1,") and ngx.shared.mock_response:get("connect")) then -- client info
			response_mock_data = add_comma_at_end(ngx.shared.mock_response:get("connect"))
			key = "connect"
		elseif (string.find(client_data, ":2002 occ 1,") and ngx.shared.load_based_mock:get("connect:"..ngx.var.server_port)) then
            response_mock_data = add_comma_at_end(ngx.shared.load_based_mock:get("connect:"..ngx.var.server_port))
            key = "connect"
		elseif (string.find(client_data, ":2004 ") and ngx.shared.mock_response:get("accept")) then -- custom challenge
			response_mock_data = add_comma_at_end(ngx.shared.mock_response:get("accept"))
			key = "accept"
		elseif (string.find(client_data, ":2004 ") and ngx.shared.mock_response:get("accept:"..ngx.var.server_port)) then -- custom challenge
			response_mock_data = add_comma_at_end(ngx.shared.mock_response:get("accept:"..ngx.var.server_port))
			key = "accept"
		else
			local keys =  ngx.shared.mock_response:get_keys();
		    local combined_key = false
			for x, value in pairs(keys) do
			    local v = value
			    if string.find(value, " MKEYSEP ") then
			        local r = split(value, " MKEYSEP ")
			        if r[1] == corr_id or r[1] == ngx.var.server_port then
			            v = r[2]
						combined_key = true
			        end
				end

				-- in case of performance test mock commit or rollback to ba always success
				if (value == "MOCK_COMMIT_FOREVER" and client_data == "8,") or
						(value == "MOCK_ROLLBACK_FOREVER" and client_data == "9,") then -- mocking COMMIT or ROLLBACK for FOREVER run
					log_to_file(ngx.DEBUG, log_id .. " got Mock for " .. v .. "client data " .. client_data .. " " .. response_mock_data)
					response_mock_data = add_comma_at_end(ngx.shared.mock_response:get(v))
				end

                if(string.find(client_data, ":25 ") and
                        (string.find(client_data, " " .. v .. " ", 1, true) or
                                string.find(client_data, " " .. v .. ".cq ", 1, true))) then
                    if combined_key == true then
                        v = value
                    end
                    if ngx.shared.load_based_mock:get(v) ~= nil then
                        response_mock_data = add_comma_at_end(ngx.shared.load_based_mock:get(v))
                    end
                    if response_mock_data == nil or string.len(response_mock_data) == 0 then
                        response_mock_data = add_comma_at_end(ngx.shared.mock_response:get(v))
                    end
                    key = v
                    if response_mock_data ~= nil and string.len(response_mock_data) > 0 then
                        local delimiter = " NEXT_COMMAND_REPLY "
                        local resp = split(response_mock_data, delimiter)
                        local query_counter = ngx.shared.worker_sql_state:get(v)

                        if query_counter == nil then
                            query_counter = 1
                        end

                        if resp[query_counter] == nil then
                            response_mock_data = ""
                            break
                        end

                        log_to_file(ngx.DEBUG, log_id .. " got Mock for " .. v .. " count " .. query_counter .. " " .. response_mock_data)
                        if resp[query_counter] == "NOMOCK" then
                            response_mock_data = ""
                            local _, error,_ = ngx.shared.worker_sql_state:set(v, query_counter+1, 600)
                            if (error ~= nil ) then
                                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                            end
                        elseif resp[query_counter] == "FOREVER" then
                            response_mock_data = resp[query_counter+1]
                            forever_mock = true
                        elseif resp[query_counter] == "FOREVER_RANDOM" then
                            response_mock_data = get_random_resp(v, resp[query_counter+1], log_id)
                            forever_mock = true
                        elseif not ends_with(resp[query_counter], " NEXT_NEWSTRING") then
                            response_mock_data = resp[query_counter]
                            local _, error,_ = ngx.shared.worker_sql_state:set(v, query_counter+1, 600)
                            if (error ~= nil ) then
                                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                            end
                        end
                        break
                    end
                end
			end
		end
	end

	if string.len(response_mock_data) > 0 then
		local a = require("mock_constant_response")
		if a.get(response_mock_data) then
			log_to_file(ngx.DEBUG, log_id .. " setting future mock in this connection ".. sock_id .. " to " .. response_mock_data)
			local _, error,_ = ngx.shared.mock_connection:set(sock_id, response_mock_data, 600)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
			_, error,_ = ngx.shared.mock_corr_connection:set(key, sock_id, 120)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
		end
	end

	response_mock_data = check_for_commands(client_data, response_mock_data, log_id, sock_id)
	if captured_response == false and check_and_capture_request(key, response_mock_data, r_sock, client_data, log_id,
			sock_id, 0) then
		response_mock_data = ""
	else
		if response_mock_data == "" then
			response_mock_data = check_for_connection_replay(sock_id, log_id)
		end
		if response_mock_data == "REPLAY_MODE_RESPONSE," then
			local key_size = 1
			local ttl = 0;
			response_mock_data, ttl, key_size = check_replay_mode(client_data, multi_keys, log_id)

			if response_mock_data == "REPLAY_MODE_RESPONSE," then
			    if r_sock == "mocked" then
			        local c_id = ngx.shared.mock_connection_corr:get(sock_id)
            	    if (c_id ~= nil) then
                        local _, error,_ = ngx.shared.response_status:set(c_id, "failed: " .. client_data, 600)
                        if (error ~= nil ) then
                            log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                        end
                    end
                end
				log_to_file(ngx.ERR, log_id .. "calling actual hera as unable to find mock for " .. client_data)
				check_and_capture_request(corr_id:sub(0,3) .. "_" .. key_size,
						"INLINE_CAPTURE", r_sock, client_data, log_id, sock_id, ttl)
				response_mock_data = ""
			end
		end

	end
	response_mock_data = check_object_mock_trim_meta_data(response_mock_data, client_data, log_id, key, forever_mock);

	if string.len(response_mock_data) > 0 then
		log_to_file(ngx.DEBUG, log_id .. "============end of get mock: " .. response_mock_data)
	end
	return add_comma_at_end(response_mock_data), corr_id
end

local function timeout_sleep(keyword, sock, log_id)
	if keyword == "response_timeout" then
		local cnt = 0
		log_to_file(ngx.INFO, log_id .. " Simulating Timeout - not sending data to Server - Respond Processing")
		while(cnt < 120) do
			ngx.sleep(2.33)
			send_net_string_data(sock, "client1", 1, "7,", log_id)
			cnt = cnt + 1
		end
	else
		log_to_file(ngx.INFO, log_id .. " Simulating Timeout - not sending data to Server")
		--ngx.sleep(120)
		--log_to_file(ngx.DEBUG, log_id .. " END sleeping 120 seconds to create read/connect timeout")
	end
end

local function check_and_simulate_timeout(mock_data, log_id, sock, red)
	local delimiter = " NEXT_NEWSTRING "
	local timeout = ""
	for line in (mock_data..delimiter):gmatch("(.-)"..delimiter) do
		if string.find(line, "response_timeout") then
			timeout = "timed-out"
			return "response_timeout", timeout
		elseif string.find(line, "timeout") and string.find(line, "backlog timeout") == nil then
			timeout = "timed-out"
			return "timeout", timeout
		elseif string.find(line, " MOCK_DELAYED_RESPONSE ") then
			local r = split(line, " MOCK_DELAYED_RESPONSE ")
			log_to_file(ngx.DEBUG, line)
			log_to_file(ngx.DEBUG, r[2])
			local t = tonumber(r[1])/1000
			log_to_file(ngx.DEBUG, log_id .. " START sleeping " .. t .. " seconds")
			timeout = "delayed " .. t .. " seconds"
			ngx.sleep(t)
			if r[2] == "NOMOCK" or r[2] == "NOMOCK," then
				return "", timeout
			end
			return r[2], timeout
		end
		break
	end
	return mock_data, timeout
end


local function read_and_forward(sender_socket, sender_name, recp_socket, recipient_name, msg, mock_data, log_id, red)
	local client_data_size, client_data
	local m_data = ""
	local corr_id = ""
    local timeout = ""
	-- if we are in _reverse direction meaning, if we are sending the data back to client and we have mock data
	-- we dont need to read the response from server, instead use the mock data as the data read from server
	if string.len(mock_data) > 0 and string.find(msg, "_reverse") then
		client_data = mock_data
		client_data_size = string.len(mock_data) - 1 --subtracting comma
	else
		-- read the data from client or server based on the port object
		client_data_size, client_data = read_net_string_data(sender_socket, sender_name, log_id)
		if not client_data_size then
			log_to_file(ngx.DEBUG, log_id .. " no data to read read_net_string_data")
			return nil, nil, m_data, corr_id, timeout
		end

		if client_data == nil then
			return client_data_size, client_data, m_data, corr_id, timeout
		end
		-- check if the data read from the client has to be mocked or not. if the response has m_data some value
		-- which means we have to mock the response
		m_data, corr_id = get_mock_data(client_data, log_id, sender_socket, get_id(recp_socket))
	end

	-- in case mock keyword is timeout, sleep on this thread and then remove the mock and go as normal.
	if string.len(m_data) > 0 then
		m_data, timeout = check_and_simulate_timeout(m_data, log_id, sender_socket, red)
	end

	if (string.len(m_data) > 0 or string.find(mock_data, "NEXT_COMMAND_REPLY "))
			and string.find(msg, "_forward") then
		-- if we have mock response and data is going to server - block the data going to server as this will be a mock
		-- response to client not the original response
		log_to_file(ngx.DEBUG, log_id .. " MOCK MOCK ... Message not send to server as mocked ")
		return client_data_size, client_data, m_data, corr_id, timeout
	end

	-- in case of this is not mocked request - allow it to go to server, so server will process it and send the response
	if (recp_socket == "mocked") then
	    local c_id = ngx.shared.mock_connection_corr:get(get_id(sender_socket))
	    if (c_id ~= nil) then
            local _, error,_ = ngx.shared.response_status:set(c_id, "failed: " .. client_data)
            if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
        end
    end
	local _, up_err = send_net_string_data(recp_socket, recipient_name, client_data_size, client_data, log_id)
	if up_err then
		log_to_file(ngx.DEBUG, log_id .. " no data to send send_net_string_data")
		return nil, nil, m_data, corr_id, timeout
	end
	return client_data_size, client_data, m_data, corr_id, timeout
end

local function read_forward_seq(connection_from_client, client_name, connection_to_server, server_name, msg,
mock_data, log_id, red)

	local key = "_forward"
	if msg == "response" then
		key = "_reverse"
	end

	local data_size, client_data, response_mock_data, corr_id, timeout = read_and_forward(connection_from_client, client_name,
		connection_to_server, server_name, msg .. ": " .. key, mock_data, log_id, red)

	if not data_size then
		return response_mock_data, nil, nil, ""
	end

	return response_mock_data, data_size, client_data, corr_id, timeout
end

local function send_mock_resp(m_data, up_sock, sock, log_id, red)
	local delimiter = " NEXT_NEWSTRING "
	local timeout = ""
	for line in (m_data..delimiter):gmatch("(.-)"..delimiter) do
		line, timeout = check_and_simulate_timeout(line, log_id, sock, red)
		if timeout ~= "" then
			collect_response_for_redis(log_id, sock ,red, "HERAMOCK: ".. timeout)
		end
		if string.find(line, "NEXT_COMMAND_REPLY ") then
			log_to_file(ngx.DEBUG, log_id .. " READ and Not sending to server as mocked " .. line)
			local _, data_size, data, _, _ = read_forward_seq(sock, "upstream", up_sock, "client1", "next_command_to_mock",
				line, log_id, red)
			log_to_file(ngx.DEBUG, log_id .. " READ and Not sending to server as mocked " .. data)
			line = line:gsub("NEXT_COMMAND_REPLY ", "")
		end
		if line ~= "" then
		    if string.find(line, "CLOSE_SOCKET") then
                log_to_file(ngx.DEBUG, log_id .. " closing socket between server and client as instructed in mock")
                return true
			end
			if not (starts_with(line, "5 randomStage_occ-user") or starts_with(line, "1001 testValue") or line == "1002,") then
		    	log_to_file(ngx.INFO, log_id .. " USING MOCK DATA *" .. line)
			end
			collect_response_for_redis(log_id, up_sock ,red, "HERAMOCK:"..line)
		    read_forward_seq(up_sock, "upstream", sock, "client1", "response", line, log_id, red)
		end
	end
	return false
end

local function cal_data(mock_data, data, data_size, log_id)
	local name = "DATA"
	local payload = "data=" .. data_size .. ":" .. data .. "&id=" .. log_id
	local do_app_log = true
	if string.find(data, "2004 ") then
		name = "CLIENT_CHALLENGE_RESPONSE"
		payload = "id=" .. log_id
		do_app_log = false
	elseif string.find(data, "1001 ") then
		name = "SERVER_CHALLENGE"
		payload = "id=" .. log_id
		do_app_log = false
	elseif data == "1002," then
		name = "SERVER_CONNECTION_ACCEPTED"
		do_app_log = false
	elseif string.find(data, "1003 ") then
		name = "SERVER_CONNECTION_REJECTED_PROTOCOL"
	elseif string.find(data, "1005 ") then
		name = "SERVER_CONNECTION_REJECTED_FAILED_AUTH"
	elseif data == "8," then
		name = "COMMIT"
	elseif data == "9," then
		name = "ROLLBACK"
	elseif string.find(data, ":25 ") then
		name = "EXEC"
	elseif string.find(data, ", Poolname: ") then
		name = "CLIENT_INFO"
		do_app_log = false
	elseif string.find(data, "Host=") then
		name = "SERVER_INFO"
		do_app_log = false
	elseif string.find(data, ":2002 ") then
		name = "CLIENT_PROTOCOL_NAME"
		do_app_log = false
	elseif data == "1008 ," then
		name = "CLIENT_PING"
		do_app_log = false
	elseif data == "1009," then
		name = "SERVER_PONG"
		do_app_log = false
	elseif string.len(mock_data) > 0 then
		name = "MSG_NOT_SEND_TO_SERVER"
	end
	return name, payload, do_app_log
end

local function capture_redis(red, sock)
	local k = ngx.shared.redis_req_res:get(sock)
	if(k ~= nil) then
	    local r_response = ngx.shared.redis_response:get(k)
		if (r_response ~= nil ) then
			local d = r_response .. ' HERAMOCK_END_TIME ' .. tostring(ngx.var.msec)
			d = d .. ' HERA_MOCK_PORT ' .. ngx.var.server_port
			red:set(k, d)
			red:expire(k, 60*60*24)
		end
		ngx.shared.redis_req_res:delete(sock)
		ngx.shared.redis_response:delete(k)
	end
end

local function read_loop(sock, up_sock, from_stream, to_stream, key, log_id, red)
	while true do
		local mock_data, data_size, data, _, timeout = read_forward_seq(sock, from_stream, up_sock, to_stream,
		    key, "", log_id, red)
		if data and ngx.shared.mock_response:get("DISABLE_LOG") == nil then
			if ngx.shared.redis_req_res:get(sock) ~= nil and key == "RESPONSE" then
				collect_response_for_redis(log_id, sock ,red, data)
			elseif (key ~= "RESPONSE") then
				if ngx.shared.redis_req_res:get(up_sock) ~= nil then
					capture_redis(red, up_sock)
				end
				local k = log_request_to_redis(log_id, data)
				local _, error,_ = ngx.shared.redis_req_res:set(up_sock, k, 600)
				if (error ~= nil ) then
                    log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                end
				if timeout ~= "" then
					collect_response_for_redis(log_id, up_sock ,red, "HERAMOCK: " .. timeout)
				end
				--else
				--	if(ngx.shared.redis_req_res:get(sock) ~= nil) then
				--		capture_redis(red, sock)
				--	end
			end
			local _, payload, do_app_log = cal_data(mock_data, data, data_size, log_id)
			if do_app_log then
				log_to_file(ngx.INFO, log_id .. " " .. key  .. " ============ *" .. payload .. "*")
			else
				log_to_file(ngx.DEBUG, log_id .. " " .. key  .. " ============ *" .. payload .. "*")
			end
		end

		-- read forward when reading from client if there was a mock setup for the read data would return the mock response
		-- below code send the response back to client. Some times client would ask to close the socket after sending
		-- the mock data. Just breaking the loop will close the socket from server and client side
		if string.len(mock_data) > 0 then
			if mock_data == "response_timeout" or mock_data == "timeout" then
				timeout_sleep(keyword, sock, log_id)
			else
				local socket_close = send_mock_resp(mock_data, up_sock, sock, log_id, red)
				if socket_close == true then
					return
				end
			end
		end
		if not data_size then
		    ngx.shared.mock_corr_connection:delete(get_id(sock))
		    ngx.shared.mock_connection:get(get_id(sock))
			log_to_file(ngx.DEBUG, log_id .. " CLOSING SOCKETS FOR " .. key .. ", id: " .. get_id(sock))
			return
		end
    end
end

local function check_data_from_client(sock, up_sock, log_id, red)
	-- read loop is a simple method which reads from first parameter fd and writes it into second parameter fd
	local th, err =  ngx.thread.spawn(read_loop, sock, up_sock, "client", "upstream", "REQUEST", log_id, red)
	return th, err
end


local function check_for_data_from_server(sock, up_sock, log_id, red)

	-- read loop is a simple method which reads from first parameter fd and writes it into second parameter fd
	local th, err = ngx.thread.spawn(read_loop, up_sock, sock, "upstream", "client1", "RESPONSE", log_id, red)
	return th, err
end

local function start_reading_from_connections(sock , up_sock, sock_to_up_sock, up_sock_to_sock, red)
	-- read data from server to client
    local thread1, notrequired
    if up_sock ~= "mocked" then
		log_to_file(ngx.DEBUG, "Start listening to server " .. get_id(up_sock))
        thread1, notrequired = check_for_data_from_server(sock, up_sock, up_sock_to_sock, red)
    end
    ngx.shared.mock_corr_connection:delete(get_id(sock))
    ngx.shared.mock_connection:get(get_id(sock))
	log_to_file(ngx.DEBUG, "Start listening to client " .. get_id(sock))
	-- read data  from client to server
	local thread2, _  = check_data_from_client(sock, up_sock, sock_to_up_sock, red)

	-- wait for client to close the connection
	ngx.thread.wait(thread2)
	if up_sock ~= "mocked" then
		log_to_file(ngx.DEBUG, "Stop listening to client " .. get_id(sock))
		up_sock:close()
	end

	-- then kill server side connection
    if thread1 ~= nil then
		capture_redis(red, up_sock)
        ngx.thread.kill(thread1)
        log_to_file(ngx.DEBUG, "Stop listening to server " .. get_id(up_sock))
    end
end


local function get_next_cal_thread_id(log_id, sock)
	local id = tonumber(string.sub(sock, -2), 16)
	while true do
		local current = ngx.shared.conn_cal_thread_id:get(id)
		if current ~= nil and current ~= sock and id < 3 then
			id = id + 1
		else
			local _, error,_ = ngx.shared.conn_cal_thread_id:set(id, sock, 600)
			if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
			return id;
		end
	end
end

local function delete_previous_load_mock(log_id, record, prev_record)
    if prev_record ~= nil then
        for key, value in pairs(record[prev_record]) do
            ngx.shared.load_based_mock:delete(key)
            if key == "CONNECT" then
                local _, error,_ = ngx.shared.load_based_mock:set("fail_connect", 0)
                if (error ~= nil ) then
                    log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                end
            end
        end
    end
end

local function add_new_load_mock(log_id, record)
    for key, value in pairs(record) do
        if key == "CONNECT" then
            local _, error,_ = ngx.shared.load_based_mock:set("connect:"..ngx.var.server_port, value["failure_response"])
            if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
            _, error,_ = ngx.shared.load_based_mock:set("fail_connect", 1)
            if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
        else
            local new_mock_value = "FOREVER_RANDOM NEXT_COMMAND_REPLY " .. value["percent"] .. " MKEYSEP 100 MKEYSEP ";
            new_mock_value = new_mock_value .. value["success_response"] .. " MKEYSEP " .. value["failure_response"]
            local _, error,_ = ngx.shared.load_based_mock:set(key, new_mock_value)
            if (error ~= nil ) then
                log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
            end
        end
        log_to_file(ngx.DEBUG, "load_based_mock triggered: " .. key)
    end
end

local function get_load_based_mock_table()
    local data = ngx.shared.load_based_mock:get(ngx.var.server_port)
    local lbm = {}
    data = data:gsub("LOAD_BASED_MOCK ", "")
    local values = split(data, " HERAMOCK_TABLESEP ")
    local debug = ""
    for i=1,#values,5
    do
        local inner_table = {}
        local traffic_range = values[i]
        local requestId = values[i+1]
        inner_table.percent = values[i+2];
        inner_table.success_response = values[i+3];
        inner_table.failure_response = values[i+4];
        if lbm[traffic_range] == nil then
            lbm[traffic_range] = {}
        end
        lbm[traffic_range][requestId] = inner_table;
    end

    return lbm
end

local function check_for_load_based_mock(log_id)
    local nor = ngx.shared.active_conn_count:get(ngx.var.server_port);
    if nor == nil then
        return
    end
    local load_based_mock_enabled = false
    if ngx.shared.load_based_mock:get(ngx.var.server_port) ~= nil then
        local mock_table = get_load_based_mock_table()
        local prev_record = ngx.shared.load_based_mock:get("current_range")

        for range, value in pairs(mock_table) do
            local min_max = split(range, "=")
            local min = tonumber(min_max[1])
            local max = tonumber(min_max[2])
            if min == nil or max == nil then
                return
            end
            if (nor >= min or min == -1) and (nor <= max or max == -1) then
                load_based_mock_enabled = true
                if prev_record ~= range then
                    delete_previous_load_mock(log_id, mock_table, prev_record)
                    add_new_load_mock(log_id, value)
                    local _, error,_ = ngx.shared.load_based_mock:set("current_range", range)
                    if (error ~= nil ) then
                        log_to_file(ngx.ERR, log_id .. " failed to set shared memory " .. error)
                    end
                    local range_msg = range
                    if prev_record ~= nil then
                        range_msg = prev_record .. "-->" .. range
                    end
                    local connect_mock = "nil"
                    if ngx.shared.load_based_mock:get("fail_connect") ~= nil then
                        connect_mock = ngx.shared.load_based_mock:get("fail_connect")
                    end
                    log_to_file(ngx.INFO, "load_based_mock triggered: " .. range_msg .. " fail_connect: " .. connect_mock)
                end
            end
        end
        if load_based_mock_enabled == false and ngx.shared.load_based_mock:get("current_range") ~= nil then
            delete_previous_load_mock(log_id, mock_table, prev_record)
            ngx.shared.load_based_mock:delete("current_range")
        end
    end
end

--[[
	From this place onwards our main code start - rest of the above code are functions used in various cases

	LOGIC
	1. Two thread will be running (check for ngx.thread.spawn in code)
		1.1 One will be listening and reading from client side port(fd) and sending data into server side port(fd)
		1.2 Another will be listening and reading data from server side port (fd) and sending data into client side port(fd)

	2. every request from client side will be read and searched for certain keywords - based on which it will decide to
		either send the request to server or instead send mock response back to client.

	MOCKS
	1. Today we can mock based on correlation id, sql name or connection level mock
		1.1 corr_id based mock - we can capture the whole request and response send for a given correlation id and
			replay them when required
		1.2 sql name - we can mock a query send to server - queries going thru DAL has a query name in comment, we use
			that comment as keyword to identify the query to be mocked
		1.3 connection level mock - example may be fail on commit or timeout on commit or timeout on connect or auth failure.
			To identify which connection should be mocked correlation id or sql name or port will be used.
			Will explain more why fail on commit or rollback has to be connection level instead of correlation id level.
			Also request and response capture is done via connection level mock.

	DATA STRUCTURES
	Most critical data used are stored as global hash tables (ngx shared). All of them are dictionary with key and value
	1. mock_response - main data structure which has the details on the mock response that needs to be sent back to client
		1.1 KEY: KEYWORDS (corr_id/sql name) - some times special key ("accept"/"connect").
					Some cases port:accept or port:connect or port <MKEYSEP> corr_id or port <MKEYSEP> sql name
					you will learn about MKEYSEP down in further comments
					DALHERAMOCK_SERVERIP key is used to identify the server ip to connect
					PORT_DALHERAMOCK_SERVERIP key is used to identify the server ip to connect for given port
		1.2 VALUES: can be one of the following or combination of these values
			1.2.1 plain net string data - a blind net string data - lua code does not process this - it sends this data back
											to client if key in dictionary matches
			1.2.2 NEXT_NEWSTRING - This is used as separator between two or more net string data. Say for example two net string data
									separated by NEXT_NEWSTRING is given. Then lua code send the first net string data as
									one unit and waits for client to read it and then sends the next net string data
									more like new line in a text
			1.2.3 NEXT_COMMAND_REPLY - This is used to command lua code read the next input from the client before
										replying with net string data that comes after NEXT_COMMAND_REPLY.
										NOTE: this behaves differently when key is corr id and key is sql name.
										when key is correlation id, the reply mock is read and send all at once.
										Example would look like
										net string data1 NEXT_COMMAND_REPLY net string data2. if the key matches for that
										request then lua would reply net string data1 first read the next input from
										client then replies with net string data2
										When key is sql name the reply mock is read and send only when next time it gets
										the same sql name Example check the NOMOCK example down
			1.2.4 timeout - keyword for timing out the matching request
							Example: to simulate timeout for a given query with name <QNAME>
							Key: <QNAME>
							Value: timeout
			1.2.5 NOMOCK - keyword to not to mock the current request. This is used in combination with net string data.
							Example: Simulating timeout for query name <QNAME> when it comes 4th time, would look like
							Key: <QNAME>
							Value: NOMOCK NEXT_COMMAND_REPLY NOMOCK NEXT_COMMAND_REPLY NOMOCK NEXT_COMMAND_REPLY timeout
			1.2.6 MKEYSEP - used as separator port and key.
							Example: Say a query with query name <QNAME> is requested for two different hera's (say conf
							and user) and we want to fail the query coming in conf and not user, then we add port in the
							key along with QNAME. Then this keyword seperated them both
							PORT <MKEYSEP> QNAME (same is applicable for correlation id)
			1.2.7 response_timeout - this is simillar to timeout with one addition functionality. Every 10 sec it gives
										back executing net string data to client
			1.2.8 MOCKFOREVER - any query level mock will get honoured only once - if this wants to repeated for all queries
								this keywords has to be appended at the end of mock response.

			NOTE: all the mock key and response has a timeout value of 100 sec after which it gets expired
    2. mock_connection - connection level mock data is stored in this data structure
    	2.1 This dictionary is used to set future mock response. Meaning say we need to mock commit and respond with
    		error code. When commit data is sent from client to server there is no correlation id or sql name to match.
    		So we cannot mock commit with corr id, we have to mock commit based on connection id. Now how to identify
    		connection, connection is identified with either SQL name or correlation id. When we match sql name or
    		correlation id we mark this connection as commit on failure (future mock). For setting this future mock
    		we use this dat structure mock_connection.
    3. mock_corr_connection
    	3.1 this data structure hols correlation id to connection mapping
    4. mock_connection_corr
    	4.1 this data structure hols connection mapping to correlation id
    5. capture_req_resp
    	5.1 this data structure will hold all the captured request and response, when mock is requested for capture
    		on a given correlation id. This structure will be given back to client thru get call once the test case
    		is completed
    6. capture_key
    	5.2 used for capture and replay
    7. current_capture_corr - sed for capture and replay
    8. conn_cal_thread_id
    	8.1 has mapping of connection to cal thread id. Used for logging cal from mock

]]--



package.path = '/usr/local/openresty/nginx/lua_files/?.lua;' .. package.path

-- get the socket object from nginx which got connected from client (request socket object)
local sock = assert(ngx.req.socket(true))

-- get a hash value of socket object for logging purpose
local sock_id = get_id(sock)
local incr_resp, err = ngx.shared.active_conn_count:incr(ngx.var.server_port, 1)
if not incr_resp and err == "not found" then
    ngx.shared.active_conn_count:add(ngx.var.server_port, 0)
    ngx.shared.active_conn_count:incr(ngx.var.server_port, 1)
end
log_to_file(ngx.DEBUG, "active_conn_count incr: " .. ngx.shared.active_conn_count:get(ngx.var.server_port))

check_for_load_based_mock(sock_id)

local ip = ngx.var.remote_addr

local redis = require "resty.redis"
local red = redis:new()
if (ngx.shared.mock_response:get("DISABLE_LOG") == nil) then
	red:set_timeouts(1000, 1000, 1000)
	local ok, err = red:connect("127.0.0.1", 6379)
	if not ok then
		log_to_file(ngx.DEBUG, "failed to connect to redis: " .. sock_id + " " + err)
	end
end

-- if we dont have any reference to this sock_id in our mock_connection_corr data structure - then it should be
-- new connection
if ngx.shared.mock_connection_corr:get(sock_id) == nil then
	log_to_file(ngx.DEBUG, "NEW CONNECTION REQUEST: " .. sock_id)
end

-- find out which hera server should connect
local up_sock, upstream_ip = get_upstream_socket()
if up_sock ~= nil and (ngx.shared.load_based_mock:get("fail_connect") == nil or ngx.shared.load_based_mock:get("fail_connect") == 0) then
	-- get a hash value of socket object for logging purpose
	local up_sock_id = get_id(up_sock)

	-- form logging string to inform on from which port to which port the data transfers
    local up_sock_to_sock = up_sock_id .. "==>" .. sock_id
    local sock_to_up_sock = sock_id .. "==>" .. up_sock_id

	-- creates two threads one reading from client to server another from server to client
	-- note when client closes the connection, we close the server connection and control comes out of this method
	-- the other way is not true when the server close the connection, still client side thread will be running
	-- mostly we dont need to change any code here or any other place - our mock logic which needs changes will be
	-- in get_mock_data
    start_reading_from_connections(sock , up_sock, sock_to_up_sock, up_sock_to_sock, red)

	if (ngx.shared.mock_response:get("DISABLE_LOG") == nil) then
   		 log_to_file(ngx.DEBUG, "CLOSING SOCKETS FOR " .. sock_id .. ":" .. up_sock_id)
	end

    ngx.shared.mock_connection:delete(sock_id);


    ngx.shared.conn_cal_thread_id:delete(cal_thread_id)
    ngx.shared.mock_connection_corr:delete(sock_id)
elseif ngx.shared.load_based_mock:get("fail_connect") == 1 then
   log_to_file(ngx.INFO, "load_based_mock closing socket immediate")
end
ngx.shared.active_conn_count:incr(ngx.var.server_port, -1)
log_to_file(ngx.DEBUG, "active_conn_count decr: " .. ngx.shared.active_conn_count:get(ngx.var.server_port))

