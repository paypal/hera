// Copyright 2020 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
