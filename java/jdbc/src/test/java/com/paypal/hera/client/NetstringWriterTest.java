package com.paypal.hera.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.paypal.hera.client.NetstringWriter;
import com.paypal.hera.constants.OccConstants;

public class NetstringWriterTest {

	public class MyStreamEx extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			throw new IOException();
		}
		
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void cleanUp() throws SQLException {
	}
	
	@Test	
	public void test_basic() throws IOException, SQLException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		NetstringWriter ns = new NetstringWriter(baos);
		ns.add(OccConstants.OCC_PREPARE, "Select 'acb' from dual".getBytes());
		ns.add(OccConstants.OCC_EXECUTE);
		ns.flush();
		Assert.assertTrue("Netstring", baos.toString().equals("34:0 24:1 Select 'acb' from dual,1:4,,"));
		ns.reset();
		ns.close();
	}

	@Test(expected = IOException.class)
	public void test_io_ex() throws IOException, SQLException{
		NetstringWriter ns = new NetstringWriter(new MyStreamEx());
		ns.write("stuff".getBytes());
		ns.flush();
	}
}
