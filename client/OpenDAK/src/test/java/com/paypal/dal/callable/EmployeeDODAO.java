package com.paypal.dal.callable;


import static com.ebay.persistence.QueryGenerator.Type.Delete;
import static com.ebay.persistence.QueryGenerator.Type.Insert;
import static com.ebay.persistence.QueryGenerator.Type.Select;
import static com.ebay.persistence.QueryGenerator.Type.Update;

import java.util.ArrayList;
import java.util.List;

import com.ebay.persistence.DALVersion;
import com.ebay.persistence.QueryGenerator;
import com.paypal.hera.dal.DalRuntimeException;
import com.paypal.hera.dal.dao.BaseDao2;
import com.paypal.hera.dal.dao.CreateException;
import com.paypal.hera.dal.dao.FinderException;
import com.paypal.hera.dal.dao.RemoveException;
import com.paypal.hera.dal.dao.UpdateException;
import com.paypal.hera.dal.ddr.ConstantToupleProvider;
import com.paypal.hera.dal.map.BaseMap2;
import com.paypal.hera.dal.map.CallableUpdateStatement;
import com.paypal.hera.dal.map.DeleteQuery;
import com.paypal.hera.dal.map.DeleteStatement;
import com.paypal.hera.dal.map.GenericMap;
import com.paypal.hera.dal.map.InsertQuery;
import com.paypal.hera.dal.map.InsertStatement;
import com.paypal.hera.dal.map.MappingIncludesAttribute;
import com.paypal.hera.dal.map.Query;
import com.paypal.hera.dal.map.QueryEngine;
import com.paypal.hera.dal.map.ReadSet;
import com.paypal.hera.dal.map.SelectQuery;
import com.paypal.hera.dal.map.SelectStatement;
import com.paypal.hera.dal.map.TableDef;
import com.paypal.hera.dal.map.TableJoin;
import com.paypal.hera.dal.map.TableStatement;
import com.paypal.hera.dal.map.UpdateQuery;
import com.paypal.hera.dal.map.UpdateSet;
import com.paypal.hera.dal.map.UpdateStatement;
import com.paypal.hera.dal.map.UpdateableMapping;

import java.util.Date;

/**
 * <pre>
 * Title:          EmployeeDODAO
 * Description:    callable
 * Copyright:      Copyright (c) 2015
 * Company:        eBay
 * @author towu
 * @version 1.0.0
 */
@DALVersion("3.0")
public class EmployeeDODAO extends BaseDao2 {
	public final static String FINDALL = "FINDALL";
	public final static String FINDBYPK = BaseMap2.PRIMARYKEYLOOKUP;
	public final static String INSERTPK = "INSERTPK";
	public final static String UPDATEPK = "UPDATEPK";
	public final static String DELETEPK = "DELETEPK";
	public final static String UPDATESALARY = "UPDATESALARY";
	public final static String SELECTFORUPDATESALARY = "SELECTFORUPDATESALARY";
	
	private static boolean m_mapInitialized = false;
	protected static MappingIncludesAttribute[] m_ourDDRHints = {};
	private volatile static EmployeeDODAO s_instance;

	public static EmployeeDODAO getInstance() {
		if (s_instance == null)
			synchronized (EmployeeDODAO.class) {
				if (s_instance == null)
					s_instance = new EmployeeDODAO();
			}
		return s_instance;
	}

	public static enum ReadSets {
		MATCHANY(-1), FULL(-2);
		int value;

		public int getValue() {
			return value;
		}

		private ReadSets(int v) {
			value = v;
		}
	}

	public static enum UpdateSets {
		SALARYUPDATE(1), MATCHANY(-1), FULL(-2);
		int value;

		private UpdateSets(int v) {
			value = v;
		}
	}

	protected EmployeeDODAO() {
		if (!m_mapInitialized)
			initMap();
	}

	public static void initMap() {
		if (m_mapInitialized)
			return;
		GenericMap<EmployeeDO> map = GenericMap.getMap(EmployeeDO.class);
		if (map == null)
			map = new GenericMap<EmployeeDO>(EmployeeDO.class);
		map.setDalVersion("3.0");
		m_mapInitialized = true;
		try {
			map.registerToupleProvider(new ConstantToupleProvider("EMP", "xe"));
		} catch (NullPointerException e) {
			throw new DalRuntimeException("DAL not properly initialized.");
		}
		initHintGroups(map);
		map.setTableJoins(getTableJoins(map));
		map.setQueries(getRawQueries(map));
		map.setReadSets(getReadSets(map));
		map.setUpdateSets(getUpdateSets(map));
		map.init();
	}

	protected static void initHintGroups(
			@SuppressWarnings("unused") GenericMap<EmployeeDO> map) {
	}

	public EmployeeDO createLocal() {
		GenericMap<EmployeeDO> map = GenericMap
				.getInitializedMap(EmployeeDO.class);
		EmployeeDOCodeGenDoImpl protoDo = new EmployeeDOCodeGenDoImpl(this, map);
		protoDo.setLocalOnly(true);
		return protoDo;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Select, variant = "Default Select Query")
	public List<EmployeeDO> findAll() throws FinderException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl protoDo = new EmployeeDOCodeGenDoImpl();
		protoDo.setLocalOnly(true);
		List<EmployeeDO> result = new ArrayList<EmployeeDO>();
		qe.readMultiple(result, protoDo.getMap(), protoDo, FINDALL,
				ReadSets.FULL.value);
		return result;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Select, variant = "Default Select Query")
	public List<EmployeeDO> findAll(final ReadSets readset)
			throws FinderException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl protoDo = new EmployeeDOCodeGenDoImpl();
		protoDo.setLocalOnly(true);
		List<EmployeeDO> result = new ArrayList<EmployeeDO>();
		qe.readMultiple(result, protoDo.getMap(), protoDo, FINDALL,
				readset.value);
		return result;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Select, variant = "Default Select Query")
	public EmployeeDO findByEmpno(final int empno) throws FinderException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl protoDo = new EmployeeDOCodeGenDoImpl();
		protoDo.setLocalOnly(true);
		protoDo.setEmpno(empno);
		EmployeeDO result = (EmployeeDO) qe.readSingle(protoDo.getMap(),
				protoDo, FINDBYPK, ReadSets.FULL.value);
		return result;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Select, variant = "Default Select Query")
	public EmployeeDO findByEmpno(final int empno, final ReadSets readset)
			throws FinderException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl protoDo = new EmployeeDOCodeGenDoImpl();
		protoDo.setLocalOnly(true);
		protoDo.setEmpno(empno);
		EmployeeDO result = (EmployeeDO) qe.readSingle(protoDo.getMap(),
				protoDo, FINDBYPK, readset.value);
		return result;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Insert, variant = "Default Insert Query")
	public void insert(final EmployeeDO item) throws CreateException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl doImpl = (EmployeeDOCodeGenDoImpl) item;
		qe.insert(doImpl.getMap(), doImpl, INSERTPK);
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Update, variant = "Default Update Query")
	public int update(final EmployeeDO item) throws UpdateException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl doImpl = (EmployeeDOCodeGenDoImpl) item;
		int recordsUpdated = qe.update(doImpl.getMap(), doImpl, UPDATEPK);
		return recordsUpdated;
	}

	@QueryGenerator(factory = "BuiltinQueryFactory", type = Delete, variant = "Default Delete Query")
	public void delete(final EmployeeDO item) throws RemoveException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl doImpl = (EmployeeDOCodeGenDoImpl) item;
		qe.delete(doImpl.getMap(), doImpl, DELETEPK);
	}

	@QueryGenerator(factory = "CallableStatementQueryFactory", type = Update, variant = "CallableUpdateStatement with PL/SQL")
	public int updateSalary(final EmployeeDO item, final Date startDate,
			final Date endDate) throws UpdateException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl doImpl = (EmployeeDOCodeGenDoImpl) item;
		
		doImpl.setStartDate(startDate);
		doImpl.setEndDate(endDate);
		
		int recordsUpdated = qe.update(doImpl.getMap(), doImpl, UPDATEPK);
		return recordsUpdated;
	}
	
	@QueryGenerator(factory = "CallableStatementQueryFactory", type = Update, variant = "CallableUpdateStatement with PL/SQL")
	public int updateSalaryWithCursorOutBindDO(final EmployeeDO item, final Date startDate,
			final Date endDate) throws UpdateException {
		QueryEngine qe = new QueryEngine();
		EmployeeDOCodeGenDoImpl doImpl = (EmployeeDOCodeGenDoImpl) item;
		
		doImpl.setStartDate(startDate);
		doImpl.setEndDate(endDate);
		
		int recordsUpdated = qe.update(doImpl.getMap(), doImpl, SELECTFORUPDATESALARY);
		return recordsUpdated;
	}
	
	protected static Query[] getRawQueries(GenericMap<EmployeeDO> map) {
		@SuppressWarnings("unused")
		TableDef emp = map.getTableDef("EMP");
		Query[] queries = {
				new SelectQuery(
						FINDALL,
						m_ourDDRHints,
						new SelectStatement[] { new SelectStatement(
								ReadSets.MATCHANY.value,
								"SELECT /*<CALCOMMENT/>*/ <SELECTFIELDS/> FROM <TABLES/> WHERE (<JOIN/>)") }),
				new SelectQuery(
						FINDBYPK,
						m_ourDDRHints,
						new SelectStatement[] { new SelectStatement(
								ReadSets.MATCHANY.value,
								"SELECT /*<CALCOMMENT/>*/ <SELECTFIELDS/> FROM <TABLES/> WHERE E.EMPNO = :m_empno AND (<JOIN/>)") }),
				new InsertQuery(
						INSERTPK,
						m_ourDDRHints,
						new TableStatement[] { new TableStatement(
								emp,
								new InsertStatement[] { new InsertStatement(
										UpdateSets.MATCHANY.value,
										"INSERT INTO /*<CALCOMMENT/>*/ :_T_EMP <INSERTFIELDS/> ") }) }),
				new UpdateQuery(
						UPDATEPK,
						m_ourDDRHints,
						new TableStatement[] { new TableStatement(
								emp,
								new UpdateStatement[] { new UpdateStatement(
										UpdateSets.MATCHANY.value,
										"UPDATE /*<CALCOMMENT/>*/ :_T_emp SET <UPDATEFIELDS/> WHERE EMPNO = :m_empno") }) }),
				new DeleteQuery(
						DELETEPK,
						m_ourDDRHints,
						new TableStatement[] { new TableStatement(
								emp,
								new DeleteStatement[] { new DeleteStatement(
										"DELETE FROM /*<CALCOMMENT/>*/ :_T_EMP WHERE EMPNO = :m_empno") }) }),
				new UpdateQuery(
						UPDATESALARY,
						m_ourDDRHints,
						new TableStatement[] { new TableStatement(
								emp,
								new UpdateStatement[] { new CallableUpdateStatement(
										UpdateSets.MATCHANY.value,
										//"/*<CALCOMMENT/>*/ begin UPDATE :_T_EMP SET SAL = SAL + 1 WHERE HIREDATE >= :m_startDate AND HIREDATE <= :m_endDate; COMMIT; end;") }) }) };
										getSQLBlockInOutParam()) }) }),
				new UpdateQuery(
						SELECTFORUPDATESALARY,
						m_ourDDRHints,
						new TableStatement[] { new TableStatement(
								emp,
								new UpdateStatement[] { new CallableUpdateStatement(
										UpdateSets.MATCHANY.value,
										//"/*<CALCOMMENT/>*/ begin UPDATE :_T_EMP SET SAL = SAL + 1 WHERE HIREDATE >= :m_startDate AND HIREDATE <= :m_endDate; COMMIT; end;") }) }) };
										getSQLBlockCursorWithOutBindingDO()) }) }) };	
												
		return queries;
	}
	
	private static String getSQLBlockInOutParam(){
		return "DECLARE\n" + 
				"x NUMBER := 100;\n" + 
				"begin\n" + 
				"x:=2;\n" + 
				"UPDATE :_T_EMP SET SAL = SAL + x WHERE HIREDATE >= :m_startDate AND HIREDATE <= :m_endDate;\n" +  
				":_OUT_m_job := 'developer';" +
				"COMMIT;\n" + 
				"end;";

	}
	
	private static String getSQLBlockCursorWithOutBindingDO(){
		return "DECLARE\n" + 
				"x NUMBER := 100;\n" + 
				"CURSOR emp_cur IS\n" +
				"SELECT * FROM :_T_EMP WHERE HIREDATE >= :m_startDate AND HIREDATE <= :m_endDate FOR UPDATE;\n" +
				"emp_rec emp_cur %ROWTYPE; " +
				"BEGIN\n" + 
				"OPEN emp_cur; " +
				"LOOP\n" +
				"FETCH emp_cur INTO emp_rec; " +
                "EXIT WHEN emp_cur%NOTFOUND; " + 
				"UPDATE :_T_EMP EE SET SAL = SAL + x WHERE EE.empno = emp_rec.empno;\n" +       
				"END LOOP;\n" +   
				"CLOSE emp_cur; " +
				"COMMIT;\n" +
				":_OUT_m_job := 'contractor';" +
				":_OUT_m_ename := 'NewName';" +
				":_OUT_m_comm := 101;" +
				":_OUT_m_deptno := 30;" +
				"END;";
	}
	
	protected static TableJoin[] getTableJoins(
			@SuppressWarnings("unused") GenericMap<EmployeeDO> map) {
		TableJoin[] tableJoins = {};
		return tableJoins;
	}

	protected static ReadSet[] getReadSets(
			@SuppressWarnings("unused") GenericMap<EmployeeDO> map) {
				ReadSet[] readSets = { new ReadSet(ReadSets.FULL.value, null) };
				return readSets;
			}

	protected static UpdateSet[] getUpdateSets(
			@SuppressWarnings("unused") GenericMap<EmployeeDO> map) {
				UpdateableMapping salFm = (UpdateableMapping) map
						.getLocalFieldMapping(EmployeeDOCodeGenDoImpl.SAL);
				UpdateSet[] updateSets = {
						new UpdateSet(UpdateSets.SALARYUPDATE.value,
								new UpdateableMapping[] { salFm }),
						new UpdateSet(UpdateSets.FULL.value, null) };
				return updateSets;
			}
}