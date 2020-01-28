package com.paypal.dal.callable;


import com.ebay.persistence.ConstantToupleProvider;
import com.ebay.persistence.Local;
import com.paypal.hera.dal.DalDOI;

import javax.persistence.Table;
import javax.persistence.Entity;
import javax.persistence.Id;

import java.util.Date;

@Entity
@Table(name = "EMP")
@com.ebay.persistence.Table(alias = "E")
@ConstantToupleProvider("D-SJC-00531190")
public interface EmployeeDO extends DalDOI {
	@Id
	public int getEmpno();

	public void setEmpno(int empno);

	public String getEname();

	public void setEname(String ename);

	public String getJob();

	public void setJob(String job);

	public int getMgr();

	public void setMgr(int mgr);

	public Date getHiredate();

	public void setHiredate(Date hiredate);

	public double getSal();

	public void setSal(double sal);

	public double getComm();

	public void setComm(double comm);

	public int getDeptno();

	public void setDeptno(int deptno);
	
	@Local
	public Date getStartDate();
	
	public void setStartDate(Date startDate);
	
	@Local
	public Date getEndDate();
	
	public void setEndDate(Date endDate);
	
}