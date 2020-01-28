package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Enum for DAL Transaction Type
 * 
 */
public enum DalTransactionTypeEnum
{
	// allow only 1 singe DB connection; fail if attempting another one
	TYPE_SINGLE_DB, //0
	
	// many DBs are allowed; 
	// try COMMITing all of them even when some COMMITs fail
	TYPE_MULTI_DB_ON_FAILURE_COMMIT, //1
	
	// many DBs are allowed; in case of COMMIT failure on one of them, 
	//   do ROLLBACK on all remaining 
	// 	 NOTE: the previous COMMITs will NOT be undone!
	TYPE_MULTI_DB_ON_FAILURE_ROLLBACK; //2

	/**
	 * get ENUM by its value (0, 1, etc.)
	 * 
	 * @param val
	 * @return
	 */
	public static DalTransactionTypeEnum getByVal(int val)
	{
		if (val < 0 || val >= DalTransactionTypeEnum.values().length) {
			return null;
		}
		return DalTransactionTypeEnum.values()[val];
	}
	
	/**
	 * Get ENUM by its name
	 * 
	 * @param name
	 * @return
	 */
	public static DalTransactionTypeEnum getByName(String name)
	{
		for (DalTransactionTypeEnum item : DalTransactionTypeEnum.values()) {
			if (item.name().equalsIgnoreCase(name)) {
				return item;
			}
		}
		return null;
	}	
	
}
