#ifndef _OCCGLOBAL_H_
#define _OCCGLOBAL_H_

/*
  Global header file for OCC

  by Eric Huss

  Copyright 1999 Confinity
*/

//default port for the server
#define OCC_PROTOCOL_NAME       "occ 1"

#define OCC_DEFAULT_PORT_NUM    12345

/**
 * @namespace occ
 * This namespace is used for any typed constants specific to OCC.
 */
namespace occ
{
	const unsigned int MAX_SCUTTLE_BUCKETS = 1024;

	/**
	 * @enum This enum defines the OCI data types supported by OCC.
	 */
	enum DataType {
		OCC_TYPE_STRING         =0,
		OCC_TYPE_BLOB           =1,
		OCC_TYPE_CLOB           =2,
		OCC_TYPE_RAW            =3,
		OCC_TYPE_BLOB_SINGLE_ROUND           =4,
		OCC_TYPE_CLOB_SINGLE_ROUND           =5,
		OCC_TYPE_TIMESTAMP		=6,
		OCC_TYPE_TIMESTAMP_TZ	=7
	};

	/**
	 * @enum This enum describes the database role
	 * in the 2PC transaction.
	 */
	enum TransRole {
		PARTICIPANT = 0,	//!< One of the participant in the global transaction.
		POINT_SITE = 1		//!< This is the commit point site as well as the global coordinator
	};

	/**
	 * @enum This enum describes the type of Transaction for OCITransStart
	 */
	enum TransType {
		DEFAULT_COUPLING = 0,
		TIGHTLY_COUPLED = 0,
		LOOSELY_COUPLED = 1,
	};

	/**
	 * @enum This enum describes the return codes from BaseDB load functions
	 * This enum is also used in DAO code gen exception handling 
	 */
	enum RC {
		SQL_SUCCESS             =0,
		SQL_ERROR               =-1,
		SQL_PARTIAL_COMMIT      =-2,		//!< For 2PC, some participants fail to commit
		SQL_NO_DATA_FOUND       =100,
		SQL_DATA_NOT_IN_CACHE   =101,
		SQL_MAX_LIMIT_EXCEEDED  =102,
	};

	/**
	 * @enum This enum describes oracle error codes from BaseDB::ora_error_number()
	 */
	enum OCCOraErrorCode
	{
		ORA_00001_UNIQUE_CONSTRAINT_VIOLATED                =     1,
		ORA_00018_MAX_SESSIONS_EXCEEDED                     =    18,
		ORA_00020_MAX_PROCESSES_EXCEEDED                    =    20,
		ORA_00028_SESSION_KILLED                            =    28,
		ORA_00054_RESOURCE_BUSY_AND_NOWAIT_SPECIFIED        =    54,
		ORA_00055_MAX_DML_LOCKS_EXCEEDED                    =    55,
		ORA_00942_TABLE_OR_VIEW_DOES_NOT_EXIST				      =   942,
		ORA_01012_NOT_LOGGED_ON                             =  1012,
		ORA_01033_INITIALIZATION_OR_SHUTDOWN_IN_PROGRESS    =  1033,
		ORA_01034_ORACLE_NOT_AVAILABLE                      =  1034,
		ORA_01400_CANNOT_INSERT_NULL                        =  1400,
		ORA_01410_INVALID_ROWID                             =  1410,
		ORA_03113_END_OF_FILE_ON_COMMUNICATION_CHANNEL      =  3113,
		ORA_03114_NOT_CONNECTED_TO_ORACLE                   =  3114,
		ORA_04031_UNABLE_TO_ALLOCATE_SHARED_MEMORY          =  4031,
		ORA_04043_OBJECT_DOES_NOT_EXIST						          =  4043,
		ORA_04088_ERROR_DURING_EXECUTION_OF_TRIGGER   		  =  4088,
		ORA_04098_TRIGGER_INVALID_AND_FAILED_REVALIDATION   =  4098,
		ORA_24756_TXN_DOES_NOT_EXIST                        = 24756,
		ORA_24764_TXN_HAS_BEEN_HEURISTICALLY_COMMITTED      = 24764,
		ORA_24765_TXN_HAS_BEEN_HEURISTICALLY_ROLLED_BACK    = 24765,
		ORA_24766_TXN_IS_PARTLY_COMMITTED_AND_ABORTED       = 24766,
		ORA_27101_SHARED_MEMORY_REALM_DOES_NOT_EXIST        = 27101,
	};

	enum ApiVersion
	{
		V1 = 1,
		V2 = 2
	};

};

#endif
