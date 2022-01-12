local msg = ngx.var.remote_addr
if ngx.header ~= nil then
    msg = msg .. ngx.header;
end
ngx.log(ngx.INFO, msg);