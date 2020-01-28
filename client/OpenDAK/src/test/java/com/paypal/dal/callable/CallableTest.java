package com.paypal.dal.callable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.paypal.hera.dal.DalInit;


public class CallableTest {

	public static void init() throws Exception {
		System.out.println("In DalLabItemTest ...");
		DalInit.init();
		System.out.println("After DalInit.init()");
	}

	public static void printAll(Date startDate, Date endDate) throws Exception {
		List<EmployeeDO> employeeDOs = EmployeeDODAO.getInstance().findAll();
		for (EmployeeDO at : employeeDOs) {

			if (at.getHiredate().after(endDate) 
					|| at.getHiredate().before(startDate )) {
				continue;
			} 
			System.out.println("----------------------------------------");
			System.out.println("EmployeeDO Id: " + at.getEmpno());
			System.out.println("EmployeeDO Name: " + at.getEname());
			System.out.println("EmployeeDO getDeptno: " + at.getDeptno());
			System.out.println("EmployeeDO getHiredate: " + at.getHiredate());
			System.out.println("EmployeeDO getSal: " + at.getSal());
		}
		System.out.println("============================");
	}

	public static void main(String[] args) throws Exception {

		init();
		
		System.out.println(">>>>>>>before PL/SQL update");
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Date startDate = dateFormat.parse("19820102");
		Date endDate = dateFormat.parse("19901109");
		
		printAll(startDate, endDate);

		List<EmployeeDO> employeeDOs = EmployeeDODAO.getInstance().findAll();
		
		EmployeeDO proto = employeeDOs.get(0);

		int rt = EmployeeDODAO.getInstance().updateSalary(proto, startDate, endDate );
		System.out.println("PL/SQL output binding parameter m_job value:" + proto.getJob());
		System.out.println("total count:" + rt);

		System.out.println(">>>>>>>after PL/SQL update");
		printAll(startDate, endDate);
		
		System.out.println(">>>>>>>before PL/SQL select for update");
		rt = EmployeeDODAO.getInstance().updateSalaryWithCursorOutBindDO(proto, startDate, endDate );
		System.out.println("PL/SQL output binding DO parameter m_job value:" + proto.getJob());
		System.out.println("PL/SQL output binding DO parameter m_comm value:" + proto.getComm());
		System.out.println("PL/SQL output binding DO parameter m_ename value:" + proto.getEname());
		System.out.println("PL/SQL output binding DO parameter m_deptno value:" + proto.getDeptno());
		
		System.out.println(">>>>>>>after PL/SQL select for update");
		printAll(startDate, endDate);

		System.exit(0);

	}
}
