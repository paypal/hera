package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Enum for DAL Transaction Status
 * 
 */
public enum DalTransactionStatusEnum
{
	// NOTE: STATUS_MARKED_ROLLBACK will be used for expired/killed transactions
	STATUS_ACTIVE, //0
	STATUS_MARKED_ROLLBACK, //1
	STATUS_PREPARED, //2
	STATUS_COMMITTED, //3
	STATUS_ROLLEDBACK, //4
	STATUS_UNKNOWN, //5
	STATUS_NO_TRANSACTION, //6
	STATUS_PREPARING, //7
	STATUS_COMMITTING, //8
	STATUS_ROLLING_BACK; //9

	/**
	 * get ENUM by its value (0, 1, etc.)
	 * 
	 * @param val
	 * @return
	 */
	public static DalTransactionStatusEnum getByVal(int val)
	{
		if (val < 0 || val >= DalTransactionStatusEnum.values().length) {
			return null;
		}
		return DalTransactionStatusEnum.values()[val];
	}
	
	/**
	 * Get ENUM by its name
	 * 
	 * @param name
	 * @return
	 */
	public static DalTransactionStatusEnum getByName(String name)
	{
		for (DalTransactionStatusEnum item : DalTransactionStatusEnum.values()) {
			if (item.name().equalsIgnoreCase(name)) {
				return item;
			}
		}
		return null;
	}	
	
}
