#!/usr/bin/env bash
FILE="/usr/local/openresty/nginx/sbin/get_ports.py"
if test -f "$FILE"; then
  python3 /usr/local/openresty/nginx/sbin/get_ports.py
else
  if [ "X$HERA_DISABLE_SSL" = "X" ] ; then
    sed -i "s/WHAT_TO_LISTEN/listen 10102 ssl;/g" /usr/local/openresty/nginx/conf/nginx.conf;
  else
    sed -i "s/WHAT_TO_LISTEN/listen 10102;/g" /usr/local/openresty/nginx/conf/nginx.conf;
  fi
fi

FILE="/usr/local/openresty/nginx/sbin/logger.py"
if test -f "$FILE"; then
  python3 /usr/local/openresty/nginx/sbin/logger.py
fi

/usr/local/openresty/nginx/sbin/heartbeat.sh &
redis-server --loadmodule /opt/redistimeseries.so &
python3 /usr/local/local_serve r/local_server.py &
/usr/local/openresty/nginx/sbin/nginx -g 'daemon off;'
