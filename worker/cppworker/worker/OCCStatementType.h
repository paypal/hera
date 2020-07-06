#ifndef _OCC_STATEMENT_TYPE_H_
#define _OCC_STATEMENT_TYPE_H_

#include <oci.h>

namespace occ
{
	enum StatementType {
		SELECT_STMT = OCI_STMT_SELECT, //!< 1
		UPDATE_STMT = OCI_STMT_UPDATE, //!< 2
		DELETE_STMT = OCI_STMT_DELETE, //!< 3
		INSERT_STMT = OCI_STMT_INSERT, //!< 4
		CREATE_STMT = OCI_STMT_CREATE, //!< 5
		DROP_STMT   = OCI_STMT_DROP, //!< 6
		ALTER_STMT  = OCI_STMT_ALTER, //!< 7
		BEGIN_STMT  = OCI_STMT_BEGIN, //!< 8
		DECLARE_STMT = OCI_STMT_DECLARE, //!< 9
		UNKNOWN_STMT = 10,          //!< This is returned when no other return value for OC_ATTR_STMT_TYPE is appropriate.
		SELECT_FOR_UPDATE_STMT = 11,			//!< Not defined in oci.h
		CALL_STMT = 12,                  //!< Not defined in oci.h
		MERGE_STMT = 16,                  //!< Not defined in oci.h
		COMMIT_STMT = 21,           //!< Not defined in oci.h
		ROLLBACK_STMT = 17          //!< Not defined in oci.h
	};
};

#endif //_OCC_STATEMENT_TYPE_H_
