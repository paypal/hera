package com.paypal.dal.ocp.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.paypal.dal.callable.EmployeeDO;
import com.paypal.dal.callable.EmployeeDODAO;
import com.paypal.hera.dal.dao.CreateException;
import com.paypal.hera.dal.dao.FinderException;
import com.paypal.hera.dal.dao.RemoveException;
import com.paypal.hera.dal.dao.UpdateException;

public class DataManager {
	public void sleep(long time) {
		try {
			System.out.println("-- Sleeping for" + time + "ms --");
			Thread.sleep(time);
			System.out.println("-- Sleep done --");
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
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
	
	public void deleteEmployee(int empNo)
			throws RemoveException {
		EmployeeDO e = EmployeeDODAO.getInstance().createLocal();
		e.setEmpno(empNo);
		EmployeeDODAO.getInstance().delete(e);
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

}
