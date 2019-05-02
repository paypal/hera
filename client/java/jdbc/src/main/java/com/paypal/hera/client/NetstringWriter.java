package com.paypal.hera.client;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.util.NetStringConstants;
import com.paypal.hera.util.HeraJdbcConverter;

public class NetstringWriter{
	ByteArrayOutputStream baos;
	OutputStream out;
	byte[] tmp_buff;
	byte[] tmp_buff2;
	static private final int TMP_BUFF_LEN = 20;
	int cmd_count = 0;

	public NetstringWriter(OutputStream _out) {
		out = new BufferedOutputStream(_out);
		baos = new ByteArrayOutputStream();
		tmp_buff = new byte[TMP_BUFF_LEN];
		tmp_buff2 = new byte[TMP_BUFF_LEN];
	}

	public void add(byte[] _cmd, long _data) throws HeraIOException {
		try {
			int pos2 = HeraJdbcConverter.long2hera(_data, tmp_buff2);
			int pos = HeraJdbcConverter.int2hera(_cmd.length + (TMP_BUFF_LEN - pos2) + 1, tmp_buff);
			baos.write(tmp_buff, pos, TMP_BUFF_LEN - pos);
			baos.write(NetStringConstants.CHARACTER_COLON);
			baos.write(_cmd);
			baos.write(NetStringConstants.CHARACTER_SPACE);
			baos.write(tmp_buff2, pos2, (TMP_BUFF_LEN - pos2));
			baos.write(NetStringConstants.CHARACTER_COMMA);
			cmd_count++;
		} catch (IOException e) {
			throw new HeraIOException(e);
		}
	}

	public void add(byte[] _cmd, byte[] _data) throws HeraIOException {
		try {
			int pos = HeraJdbcConverter.int2hera(_cmd.length + _data.length + 1, tmp_buff);
			baos.write(tmp_buff, pos, TMP_BUFF_LEN - pos);
			baos.write(NetStringConstants.CHARACTER_COLON);
			baos.write(_cmd);
			baos.write(NetStringConstants.CHARACTER_SPACE);
			baos.write(_data);
			baos.write(NetStringConstants.CHARACTER_COMMA);
			cmd_count++;
		} catch (IOException e) {
			throw new HeraIOException(e);
		}
	}

	public void add(byte[] _cmd) throws HeraIOException {
		try {
			int pos = HeraJdbcConverter.int2hera(_cmd.length, tmp_buff);
			baos.write(tmp_buff, pos, TMP_BUFF_LEN - pos);
			baos.write(NetStringConstants.CHARACTER_COLON);
			baos.write(_cmd);
			baos.write(NetStringConstants.CHARACTER_COMMA);
			cmd_count++;
		} catch (IOException e) {
			throw new HeraIOException(e);
		}
	}
	
	public void reset() {
		baos.reset();
	}

	public void write(byte[] b) throws IOException {
		out.write(b);
	}

	public void flush() throws IOException {
		if(cmd_count ==1) {
		    byte[] data = baos .toByteArray();
            out.write(data);
            out.flush();
            baos.reset();
            cmd_count = 0;
       } else {
		    byte[] data = baos .toByteArray();
            int pos = HeraJdbcConverter.int2hera(1 + 1 + data. length, tmp_buff);
            out.write( tmp_buff, pos, TMP_BUFF_LEN - pos);
            out.write(NetStringConstants. CHARACTER_COLON);
            out.write(NetStringConstants. CHARACTER_ZERO );
            out.write(NetStringConstants. CHARACTER_SPACE);
            out.write(data);
            out.write(NetStringConstants. CHARACTER_COMMA);
            out.flush();
            baos.reset();
            cmd_count = 0;
       }   

	}

	public void close() throws IOException {
		out.close();
	}

}
