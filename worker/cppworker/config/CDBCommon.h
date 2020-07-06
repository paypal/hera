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
