package com.paypal.hera.dal.cm.transaction;

import java.sql.Connection;

/**
 * 
 * Description: 	Enum for DAL Transaction Level
 * 
 */
public enum DalTransactionLevelEnum
{
	// NB: Oracle supports only READ_COMMITTED and SERIALIZABLE!!
	TRANSACTION_NONE(Connection.TRANSACTION_NONE), //0
	TRANSACTION_READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED), //1
	TRANSACTION_READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED), //2
	TRANSACTION_REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ), //4
	TRANSACTION_SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE); //8
	
	private int m_level;

	private DalTransactionLevelEnum(int value)
	{
		m_level = value;
	}
	
	public int getLevel()
	{
		return m_level;
	}


	/**
	 * get ENUM by its value (0, 1, etc.)
	 * 
	 * @param val
	 * @return
	 */
	public static DalTransactionLevelEnum getByVal(int val)
	{
		for (DalTransactionLevelEnum item : DalTransactionLevelEnum.values()) {
			if (item.m_level == val) {
				return item;
			}
		}
		return null;
	}
	
	/**
	 * Get ENUM by its name
	 * 
	 * @param name
	 * @return
	 */
	public static DalTransactionLevelEnum getByName(String name)
	{
		for (DalTransactionLevelEnum item : DalTransactionLevelEnum.values()) {
			if (item.name().equalsIgnoreCase(name)) {
				return item;
			}
		}
		return null;
	}	
	
}
