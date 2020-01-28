package com.paypal.dal.callable;


import java.util.Date;

import com.ebay.persistence.DALVersion;
import com.paypal.hera.dal.BaseDo3;
import com.paypal.hera.dal.dao.BaseDao2;
import com.paypal.hera.dal.map.BaseMap2;
import com.paypal.hera.dal.map.GenericMap;

/**
 * <pre>
 * Title:          EmployeeDOCodeGenDoImpl
 * Description:    callable
 * Copyright:      Copyright (c) 2015
 * Company:        eBay
 * @author towu
 * @version 1.0.0
 * </pre>
 */
@DALVersion("3.0")
public class EmployeeDOCodeGenDoImpl extends BaseDo3 implements EmployeeDO {
	public final static long serialVersionUID = 1L;
	public final static int EMPNO = BaseDo3.NUM_FIELDS + 0;
	public final static int ENAME = BaseDo3.NUM_FIELDS + 1;
	public final static int JOB = BaseDo3.NUM_FIELDS + 2;
	public final static int MGR = BaseDo3.NUM_FIELDS + 3;
	public final static int HIREDATE = BaseDo3.NUM_FIELDS + 4;
	public final static int SAL = BaseDo3.NUM_FIELDS + 5;
	public final static int COMM = BaseDo3.NUM_FIELDS + 6;
	public final static int DEPTNO = BaseDo3.NUM_FIELDS + 7;
	public final static int STARTDATE = BaseDo3.NUM_FIELDS + 8;
	public final static int ENDDATE = BaseDo3.NUM_FIELDS + 9;
	@SuppressWarnings("hiding")
	public final static int NUM_FIELDS = BaseDo3.NUM_FIELDS + 10;
	@SuppressWarnings("hiding")
	public final static int NUM_SUBOBJECT_FIELDS = BaseDo3.NUM_SUBOBJECT_FIELDS + 0;
	public Date m_endDate;
	public Date m_startDate;
	public int m_deptno;
	public double m_comm;
	public double m_sal;
	public Date m_hiredate;
	public int m_mgr;
	public String m_job;
	public String m_ename;
	public int m_empno;

	public EmployeeDOCodeGenDoImpl() {
		super(EmployeeDODAO.getInstance(), GenericMap
				.getInitializedMap(EmployeeDO.class));
	}

	public EmployeeDOCodeGenDoImpl(BaseDao2 dao, BaseMap2 map) {
		super(dao, map);
	}

	public Date getEndDate() {
		return m_endDate;
	}

	public void setEndDate(Date p_endDate) {
		this.m_endDate = p_endDate;
		setDirty(ENDDATE);
	}

	public Date getStartDate() {
		return m_startDate;
	}

	public void setStartDate(Date p_startDate) {
		this.m_startDate = p_startDate;
		setDirty(STARTDATE);
	}

	public int getDeptno() {
		loadValue(DEPTNO);
		return m_deptno;
	}

	public void setDeptno(int p_deptno) {
		this.m_deptno = p_deptno;
		setDirty(DEPTNO);
	}

	public double getComm() {
		loadValue(COMM);
		return m_comm;
	}

	public void setComm(double p_comm) {
		this.m_comm = p_comm;
		setDirty(COMM);
	}

	public double getSal() {
		loadValue(SAL);
		return m_sal;
	}

	public void setSal(double p_sal) {
		this.m_sal = p_sal;
		setDirty(SAL);
	}

	public Date getHiredate() {
		loadValue(HIREDATE);
		return m_hiredate;
	}

	public void setHiredate(Date p_hiredate) {
		this.m_hiredate = p_hiredate;
		setDirty(HIREDATE);
	}

	public int getMgr() {
		loadValue(MGR);
		return m_mgr;
	}

	public void setMgr(int p_mgr) {
		this.m_mgr = p_mgr;
		setDirty(MGR);
	}

	public String getJob() {
		loadValue(JOB);
		return m_job;
	}

	public void setJob(String p_job) {
		this.m_job = p_job;
		setDirty(JOB);
	}

	public String getEname() {
		loadValue(ENAME);
		return m_ename;
	}

	public void setEname(String p_ename) {
		this.m_ename = p_ename;
		setDirty(ENAME);
	}

	public int getEmpno() {
		loadValue(EMPNO);
		return m_empno;
	}

	public void setEmpno(int p_empno) {
		this.m_empno = p_empno;
		setDirty(EMPNO);
	}

	@Override
	public int getNumFields() {
		return NUM_FIELDS;
	}

	@Override
	public int getNumSubObjects() {
		return NUM_SUBOBJECT_FIELDS;
	}
}