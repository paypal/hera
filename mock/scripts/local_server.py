
try:
    # For Python 3.0 and later
    from http.server import SimpleHTTPRequestHandler
    import socketserver as SocketServer
except ImportError:
    # Fall back to Python 2's urllib2
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    import SocketServer

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

import logging
import redis
import json
import collections
import datetime
import ssl
import time

PORT = 8200

column_name_cache = dict()
port_hash = dict()


class NetString:

    @staticmethod
    def hera_commands(data):
        commands = {
            "8,": "Commit",
            "9,": "RollBack",
            "5,": "Success",
            "7": "Fetch"
        }
        if len(data.split()) > 1:
            return commands.get(data.split()[0], data) + " " + " ".join(data.split()[1:])
        return commands.get(data, data)

    @staticmethod
    def hera_value(data):
        value = None
        l = int(data[:data.index(":")].strip())
        data = data[data.index(":")+1:]
        rest = data[:l]
        if rest.split()[0] == "3":
            if len(rest.split()) == 1:
                value = ""
            else:
                value = rest.split()[1].strip()
        data = data[l+1:]
        return data, value

    @staticmethod
    def is_column_details(data, ref_cols):
        try:
            if data.split()[0] == "0":
                data = data[1:].strip()
            else:
                return False
            data, c = NetString.hera_value(data)
            if int(c) == ref_cols:
                return True
        except:
            pass
        return False

    @staticmethod
    def get_column_values(data, cols):
        values = list()
        if data.split()[0] == "0":
            data = data[1:].strip()
        else:
            return values
        l = len(cols)
        idx = 0
        rec = dict()
        while len(data) > 0:
            if data == ",":
                break
            data, val = NetString.hera_value(data)
            if l > 0:
                rec[cols[idx]] = val
            else:
                rec[idx] = val
            idx += 1
            if 0 < l <= idx:
                values.append(rec)
                rec = dict()
                idx = 0
        if len(rec) > 0:
            values.append(rec)
        return values


    @staticmethod
    def get_column_details(data, query):
        cols = list()
        if data.split()[0] == "0":
            data = data[1:].strip()
        data, c = NetString.hera_value(data)
        while len(data) > 0:
            if data == ",":
                break
            data, val = NetString.hera_value(data)
            cols.append(val)
            data, val = NetString.hera_value(data)
            data, val = NetString.hera_value(data)
            data, val = NetString.hera_value(data)
            data, val = NetString.hera_value(data)
        column_name_cache[query] = cols
        return cols

    @staticmethod
    def read_meta(data):
        if data.split()[0] == "0":
            data = data[1:].strip()
        else:
            return -1, -1, data[1:].strip()

        data, cols = NetString.hera_value(data)
        data, rows = NetString.hera_value(data)
        return int(cols), int(rows), None


class GetHandler(SimpleHTTPRequestHandler):

    def write_json(self, data, reverse=False, readable = False):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if reverse is True:
            reverse_sorted_data = collections.OrderedDict(sorted(data.items(), reverse=True))
            self.wfile.write(json.dumps(reverse_sorted_data, indent=4, ensure_ascii=False).encode())
            return
        if readable == "true":
            resp = dict()
            for rec in data:
                resp[datetime.datetime.fromtimestamp(int(rec)/1000).strftime("%H:%M:%S")] = data[rec]
            data = resp
        self.wfile.write(json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False).encode())

    @staticmethod
    def get_service_name(port):
        try:
            if port_hash.get(port) is None:
                request = "https://topo.es.paypalcorp.com/package/get_port_info/?port_numbers=" + str(port)
                response = json.loads(urlopen(request, context=ssl._create_unverified_context()).read().decode())
                port_hash[port] = response["port_info"]["stage"][0]["packagename"]
        except:
            port_hash[port] = port
        return port_hash[port]


    @staticmethod
    def delete_data(search_string):
        red = redis.Redis(host='localhost', port=6379, db=0)
        for key in red.scan_iter(search_string):
            red.delete(key)
        return GetHandler.get_data("*")

    @staticmethod
    def get_bind_in(pl, query):
        res = dict()
        bind = pl.split(query + ",")[1]
        key = None
        val = None
        while len(bind) > 0 and bind.find(":") > 0:
            if bind == ",":
                break
            col = bind.index(':')
            length = int(bind[:col])
            values = bind[col+1:col+1+length].split()
            if key is not None and val is not None:
                res[key] = val.strip()
                key = val = None

            if values[0] == "2":
                key = values[1]
            if values[0] == "3":
                if len(values) == 1:
                    val = ""
                else:
                    val = " ".join(values[1:])
            bind = bind[length+col+2:]
        return res

    @staticmethod
    def get_bind_out(response, query):
        i = 0
        cols = rows = 0
        column_details = list()
        column_values = list()
        for line in response.split(" NEXT_NEWSTRING "):
            if i == 0:
                cols, rows, err = NetString.read_meta(line)
            else:
                if i == 1 and NetString.is_column_details(line, cols) is True:
                    column_details = NetString.get_column_details(line, query)
                else:
                    if column_details is None or len(column_details) == 0:
                        column_details = column_name_cache.get(query, list())
                    cv = NetString.get_column_values(line, column_details)
                    if len(cv) > 0:
                        column_values.append(cv)

            i += 1
        if len(column_values) == 0 and rows >= 0:
            val = "updated"
            if rows == 0 and cols > 0:
                val = "returned"
            column_values.append("%s rows %s" % (rows, val))
        if err is not None:
            column_values.append(err)
        return column_values

    @staticmethod
    def get_next_field(data):
        first = data[:data.index(':')]
        rest = data[data.index(':')+1:]
        return GetHandler.decode(first), GetHandler.decode(rest)

    @staticmethod
    def get_data_time_sorted(search_string):
        d = GetHandler.get_data(search_string)
        resp = dict()
        for h in d:
            sql = d.get(h).get("SQL")
            for t in d.get(h):
                if t == "SQL":
                    continue
                r = d[h][t]
                qt = r["queryTime"]
                r["SQL"] = sql
                r["timeStamp"] = t
                if resp.get(qt) is None:
                    resp[qt] = list()
                resp[qt].append(r)
        return resp

    @staticmethod
    def decode(inp):
        try:
            return str(inp.decode())
        except:
            return str(inp)

    @staticmethod
    def traffic(port, start):
        resp = dict()
        html_format = ""
        status = True
        try:
            red = redis.Redis(host='localhost', port=6379, db=0)
            end_time = int(time.time()*1000)
            start_time = int(time.time()*1000) - 5 * 60 * 1000
            if start is not None:
                start_time = int(start)
                end_time = start_time + 5 * 60 * 1000
            range_reply = red.eval("return redis.call('TS.RANGE', 'busy_worker_count:" + str(port) + "', " + str(start_time) + ", " + str(end_time)  + ")", 0)
            x = "["
            y = "["
            data = dict()
            starting = 0
            ending = 0
            counter = 0
            for rec in range_reply:
                resp[int(rec[0])] = rec[1]
                local_x = int(rec[0])/1000

                if starting == 0 or starting > local_x:
                    starting = local_x
                if ending < local_x:
                    ending = local_x

                if data.get(local_x, 0) < int(rec[1]):
                    data[local_x] = int(rec[1])

            for x_axis in range(starting-2, ending+3):
                y_axis = data.get(x_axis, 0)

                if x != "[":
                    x += ", "
                    y += ", "
                x += "\"" + datetime.datetime.fromtimestamp(x_axis).strftime("%H:%M:%S") + "\""
                y += str(int(y_axis))
                counter += 1
                if counter >= 5 * 60:
                    break
            x += "]"
            y += "]"

            html_format = "<!DOCTYPE html><html><script src=\"https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.5.0/" \
                          "Chart.min.js\"></script><body><canvas id=\"myChart\" style=\"width:100%\"></canvas>" \
                          "<script>var xValues = " + x + ";var yValues = " + y + ";new Chart(\"myChart\", {type: \"line\"," \
                                                                                 "data: {labels: xValues,datasets: [{fill: false,pointRadius: 2,borderColor: \"rgba(0,0,255,0.5)\"," \
                                                                                 "data: yValues}]},options: {legend: {display: false},title: {display: true,text: \"EXEC Count\"," \
                                                                                 "fontSize: 16}}});</script></body></html>"
        except Exception as e:
            print(e)
            resp['Error'] = str(e)
            status = False
        return status, resp, html_format

    @staticmethod
    def get_data(search_string):
        red = redis.Redis(host='localhost', port=6379, db=0)
        response = dict()
        for hash_query in sorted(red.keys(search_string), reverse=True):
            hash_query = GetHandler.decode(hash_query)
            command = False
            if str(hash_query).startswith("busy_worker_count"):
                continue
            split_key = hash_query
            tt, split_key = GetHandler.get_next_field(split_key)
            corr_id, split_key = GetHandler.get_next_field(split_key)
            hash_code, split_key = GetHandler.get_next_field(split_key)
            not_used, query = GetHandler.get_next_field(split_key)
            pl = GetHandler.decode(red.get(hash_query))
            if query == "Command":
                query = hash_code = NetString.hera_commands(pl.split(" START_RESPONSE ")[0])
                command = True
            if response.get(hash_code) is None:
                response[hash_code] = {"SQL": query}

            # pl = pl.decode("utf-8", errors='ignore').encode("ascii", "ignore")
            event_time = str(datetime.datetime.fromtimestamp(float(tt)).strftime('%Y-%m-%d %H:%M:%S.%f'))
            if response.get(hash_code) is None or response[hash_code].get(tt) is None:
                response[hash_code][tt] = { "queryTime": event_time, "corrId": corr_id}
            try:
                req = str(pl).split(" START_RESPONSE  NEXT_NEWSTRING ")

                if not command:
                    response[hash_code][tt]["request"] = GetHandler.get_bind_in(req[0], query)
                if len(req) > 1:
                    res = req[1].split(" HERAMOCK_END_TIME ")[0]
                    if str(res).startswith("HERAMOCK:"):
                        response[hash_code][tt]["mockedResponse"] = True
                        res = res.replace("HERAMOCK:", "")
                    if command:
                        response[hash_code][tt]["response"] = NetString.hera_commands(req[1].
                                                                                     split(" HERAMOCK_END_TIME ")[0])
                    else:
                        response[hash_code][tt]["response"] = GetHandler.get_bind_out(res, query)
                    response[hash_code][tt]["timeTaken"] = str((float(req[1].split(" HERAMOCK_END_TIME ")[1].split(' ')[0]) -
                                                                float(tt))*1000) + "ms"
                    response[hash_code][tt]["port"] = str(req[1].split(" HERA_MOCK_PORT ")[1].split(' ')[0])
                    response[hash_code][tt]["hera_name"] = GetHandler.get_service_name(req[1].split(" HERA_MOCK_PORT ")[1].split(' ')[0])
            except Exception as e:
                print(e)
                import sys, traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print(repr(traceback.format_exception(exc_type, exc_value,
                                                      exc_traceback)))
                response[hash_code][tt]["rawResponse"] = req

        return response

    def tail(self, f, lines):
        total_lines_wanted = lines

        BLOCK_SIZE = 1024
        f.seek(0, 2)
        block_end_byte = f.tell()
        lines_to_go = total_lines_wanted
        block_number = -1
        blocks = []
        while lines_to_go > 0 and block_end_byte > 0:
            if (block_end_byte - BLOCK_SIZE > 0):
                f.seek(block_number*BLOCK_SIZE, 2)
                blocks.append(f.read(BLOCK_SIZE))
            else:
                f.seek(0, 0)
                blocks.append(f.read(block_end_byte))
            lines_found = str(blocks[-1]).count('\n')
            lines_to_go -= lines_found
            block_end_byte -= BLOCK_SIZE
            block_number -= 1
        all_read_text = b''.join(reversed(blocks))
        return b'\n'.join(all_read_text.splitlines()[-total_lines_wanted:])


    def do_GET(self):
        logging.error(self.headers)
        try:
            from urllib.parse import urlparse
        except:
            from urlparse import urlparse
        parsed = urlparse(self.path)
        query = parsed.query
        query_components = dict()
        if parsed.path in ["/mock/logs", "/",  "/mock/logs/"]:
            if str(query).find("=") >= 0:
                query_components = dict(qc.split("=") for qc in query.split("&"))
            key = query_components.get("key", "*")
            delete = str(query_components.get("delete", "false")).lower()
            if delete == "true":
                self.write_json(GetHandler.delete_data(key))
            else:
                self.write_json(GetHandler.get_data_time_sorted(key), True)
            return
        elif parsed.path in ["/mock/rawlogs", "/mock/rawlogs/"]:
            f = "/usr/local/openresty/nginx/logs/stream_error.log"
            self.send_response(200)
            self.end_headers()
            # place absolute path here
            f_served = open(f,'rb')
            f_content = self.tail(f_served, 10000)
            f_served.close()
            self.wfile.write(f_content)
            return
        elif parsed.path in ["/mock/traffic"]:
            if str(query).find("=") >= 0:
                query_components = dict(qc.split("=") for qc in query.split("&"))
            port = str(query_components.get("port", "10101")).lower()
            html = str(query_components.get("html", "true")).lower()
            start = query_components.get("start", None)
            readable = str(query_components.get("readable", "true")).lower()
            status, resp, html_format = GetHandler.traffic(port, start)
            if html != "true" or not status :
                self.write_json(resp, False, readable)
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(html_format)
            return

Handler = GetHandler
httpd = SocketServer.TCPServer(("", PORT), Handler)

httpd.serve_forever()

