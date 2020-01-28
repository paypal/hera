package com.paypal.dal.ocp.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.paypal.dal.callable.EmployeeDO;
import com.paypal.dal.callable.EmployeeDODAO;
import com.paypal.hera.dal.DalInit;
import com.paypal.hera.dal.cm.transaction.DalTransactionException;
import com.paypal.hera.dal.cm.transaction.DalTransactionLevelEnum;
import com.paypal.hera.dal.cm.transaction.DalTransactionManager;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerFactory;
import com.paypal.hera.dal.cm.transaction.DalTransactionTypeEnum;
import com.paypal.hera.dal.dao.CreateException;
import com.paypal.hera.dal.dao.FinderException;
import com.paypal.hera.dal.dao.UpdateException;
import com.paypal.platform.security.PayPalSSLHelper;
import static org.junit.Assert.*;

public class DALTransactionTest {
	
	@Before
	public void init() throws Exception {
		PayPalSSLHelper.initializeSecurity();
		DalInit.init();
		
	}

	public void printAll(Date startDate, Date endDate) throws Exception {
		List<EmployeeDO> employeeDOs = EmployeeDODAO.getInstance().findAll();
		for (EmployeeDO at : employeeDOs) {

			// if (at.getHiredate().after(endDate)
			// || at.getHiredate().before(startDate)) {
			// continue;
			// }
			System.out.println("----------------------------------------");
			System.out.println("EmployeeDO Id: " + at.getEmpno());
			System.out.println("EmployeeDO Name: " + at.getEname());
			System.out.println("EmployeeDO getDeptno: " + at.getDeptno());
			System.out.println("EmployeeDO getHiredate: " + at.getHiredate());
			System.out.println("EmployeeDO getSal: " + at.getSal());
		}
		System.out.println("============================");
	}

	public EmployeeDO readEmployee(int empNo) throws FinderException {
		EmployeeDO proto = EmployeeDODAO.getInstance().findByEmpno(empNo);
		return proto;
	}

	public void updateEmployee(EmployeeDO proto, int salary) throws ParseException, UpdateException {
		proto.setSal(salary);
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date startDate = dateFormat.parse("19820102");
		Date endDate = dateFormat.parse("19901109");

		int rt = EmployeeDODAO.getInstance().updateSalary(proto, startDate, endDate);
		System.out.println("Records updated ---" + rt);

	}

	public void createEmployee(int empNo, String name, String job, int salary, int deptNo)
			throws CreateException {
		EmployeeDO e = EmployeeDODAO.getInstance().createLocal();
		e.setEmpno(empNo);
		e.setEname(name);
		e.setJob(job);
		e.setSal(salary);
	//	e.setDeptno(30); // Foreign key. Give non-existing department id here if
							// want to fail insert.
		EmployeeDODAO.getInstance().insert(e);
	}
	
	@Test
	public void singleDBTnx() {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();
			dalTransMgr.setTransactionType(DalTransactionTypeEnum.TYPE_SINGLE_DB);
			dalTransMgr.setTransactionTimeout(100000); // default 15 sec
			dalTransMgr.setTransactionLevel(DalTransactionLevelEnum.TRANSACTION_READ_COMMITTED);
			
			createEmployee(1114, "Emp1", "job1", 50000, 30);
			EmployeeDO empDo = readEmployee(1112);
			updateEmployee(empDo, 666);

			System.out.println("committing");
			dalTransMgr.commit();
			
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			Date startDate = dateFormat.parse("19820102");
			Date endDate = dateFormat.parse("19901109");
			printAll(startDate, endDate);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("rolling back:" + ex);
			try {
				dalTransMgr.rollback();
			} catch (DalTransactionException dte) {
				// throw dte;
			}
			// throw ex;
		}
	}
	
	@Test
	public void singleDBTnxStaleConn() {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();
			dalTransMgr.setTransactionType(DalTransactionTypeEnum.TYPE_SINGLE_DB);
			// dalTransMgr.setTransactionType(DalTransactionTypeEnum.TYPE_MULTI_DB_ON_FAILURE_ROLLBACK);
			dalTransMgr.setTransactionTimeout(100000); // default 15 sec
			dalTransMgr.setTransactionLevel(DalTransactionLevelEnum.TRANSACTION_READ_COMMITTED);
			// dalTransMgr.setTransactionLevel(DalTransactionLevelEnum.TRANSACTION_SERIALIZABLE);
			
			sleep(); //to get EOF
			EmployeeDO empDo = readEmployee(1008);
			
			//createEmployee(1008, "Emp1", "job1", 50000, 30);
			updateEmployee(empDo, 666);
			
//			EmployeeDO emp = readEmployee(1008);
			
			System.out.println("committing");
			dalTransMgr.commit();
			
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			Date startDate = dateFormat.parse("19820102");
			Date endDate = dateFormat.parse("19901109");
			printAll(startDate, endDate);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("rolling back:" + ex);
			try {
				dalTransMgr.rollback();
			} catch (DalTransactionException dte) {
				// throw dte;
			}
			// throw ex;
		}
	}
	
	
	@Test
	public void singleDBTnxTimeout() {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();
			dalTransMgr.setTransactionType(DalTransactionTypeEnum.TYPE_SINGLE_DB);
			dalTransMgr.setTransactionTimeout(4); // timeout after 4 sec
			dalTransMgr.setTransactionLevel(DalTransactionLevelEnum.TRANSACTION_READ_COMMITTED);
			sleep();
			EmployeeDO empDo = readEmployee(1008);
		} catch (Exception ex) {
			fail("Exception should not be thrown first time");
			ex.printStackTrace();
			System.out.println("rolling back:" + ex);
			try {
				dalTransMgr.rollback();
			} catch (DalTransactionException dte) {
				// throw dte;
			}
		}
		sleep();
		try {
			EmployeeDO empDo = readEmployee(1008); // this shd fail.
			fail("Exception should be thrown");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void sleep() {
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

}
