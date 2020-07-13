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
#ifndef CDB_COMMON_H
#define CDB_COMMON_H

/**
 * Special CDB key always included in cdbs made with cdbmake4 and later;
 * signified charset used to encode all values' bytes in this CDB.
 * If not present (cdbmake3 or earlier) charset is assumed to be
 * Windows-1252.  
 *
 * This default to Windows-1252 keeps backwards-compatibility with
 * most CDBs including German/French/etc. locale messages, but breaks
 * Japanese locale messages.  For those, use cdbmake4 so all cdbs are
 * written in UTF-8 and __cdb_charset is set to "utf-8".
 */
#define CDB_KEY_CHARSET "__cdb_charset"

#endif
