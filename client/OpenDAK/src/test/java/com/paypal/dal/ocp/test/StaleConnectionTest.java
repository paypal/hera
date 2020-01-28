package com.paypal.dal.ocp.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.paypal.dal.callable.EmployeeDO;
import com.paypal.hera.dal.DalInit;
import com.paypal.hera.dal.dao.CreateException;
import com.paypal.hera.dal.dao.FinderException;
import com.paypal.hera.dal.dao.RemoveException;
import com.paypal.platform.security.PayPalSSLHelper;

public class StaleConnectionTest {
	@Before
	public void init() throws Exception {
		DalInit.init();
		PayPalSSLHelper.initializeSecurity();
	}
	
	@Test
	public void selectTest() {
		DataManager dm = new DataManager();
		try {
			dm.deleteEmployee(1111);
			dm.createEmployee(1111, "Emp1", "job1", 50000, 30);
			dm.sleep(6000); //occ server idletimeout shd be 5ms. OCP pool idle/orphan timeout shd be more than 5ms.
			EmployeeDO emp = dm.readEmployee(1111);
			Assert.assertEquals(emp.getEmpno(), 1111);
			dm.deleteEmployee(1111);
		} catch (CreateException | FinderException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (RemoveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
}
