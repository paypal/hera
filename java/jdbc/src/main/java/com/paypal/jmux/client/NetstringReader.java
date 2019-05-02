package com.paypal.jmux.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import com.paypal.jmux.util.NetStringObj;

public class NetstringReader {
	
	private InputStream is;
	ArrayList<NetStringObj> responses;

	public NetstringReader(InputStream _is) {
		is = _is;	
		responses = new ArrayList<NetStringObj>(); 
	}

	public Iterator<NetStringObj> parse() throws IOException {
		responses.clear();
		NetStringObj obj = new NetStringObj(is);
		if (obj.getCommand() == 0) {
			ByteArrayInputStream bais = new ByteArrayInputStream(obj.getData());
	        while (bais.available() > 0) {
	            NetStringObj nso = new NetStringObj(bais);
	            responses.add(nso);
	        }
		} else {
			responses.add(obj);
		}
		return responses.iterator();
	}
}
