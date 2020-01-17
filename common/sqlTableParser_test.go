// Copyright 2019 PayPal Inc.
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

package common

import (
  "testing"
  "strings"
  "fmt"
)

const(
  sqlStr = "SELECT /* ProdConfigLayerPublicNameDOMap.FINDBYLAYERID.-2 .3*/ PCLPN.PROD_CONFIG_LAYER_NAME, PCLPN.PROD_CONFIG_LAYER_ID, PCLPN.TIME_CREATED FROM PROD_CONFIG_LAYER_PUBLIC_NAME PCLPN WHERE PCLPN.PROD_CONFIG_LAYER_ID IN (:prod_config_layer_id, :prod_config_layer_id2, :prod_config_layer_id3) AND ((1 = 1))"
  outStr = "SELECT  PCLPN.PROD_CONFIG_LAYER_NAME, PCLPN.PROD_CONFIG_LAYER_ID, PCLPN.TIME_CREATED FROM PROD_CONFIG_LAYER_PUBLIC_NAME PCLPN WHERE PCLPN.PROD_CONFIG_LAYER_ID IN (:prod_config_layer_id, :prod_config_layer_id2, :prod_config_layer_id3) AND ((1 = 1))"
  sqlStr2 = "SELECT column-names FROM table-name1 INNER JOIN table-name2 ON column-name1 = column-name2 WHERE condition"
  sqlStr3 = "SELECT /* JoinedMap.IS_MAM_ACCOUNT_BY_SUBJECT_ACCOUNT.1 */ WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WRSM.USER_REL_ID FROM (    SELECT WR.UPDATE_VERSION FROM    (        SELECT WR.STATUS, rownum r__ FROM (SELECT /* JoinedMap.IS_MAM_ACCOUNT_BY_SUBJECT_ACCOUNT.1 */WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WR.ACCOUNT_NUMBER_ONE, WR.ACCOUNT_NUMBER_TWO FROM WUSER_RELATIONSHIP WR JOIN WUSER_RELATION_SERVICE_MAP WRSM ON WR.ACCOUNT_NUMBER = WRSM.ACCOUNT_NUMBER AND WR.ID = WRSM.USER_REL_ID WHERE WR.ACCOUNT_NUMBER = :account_number AND WR.STATUS = 'A' AND WR.TYPE = :type AND WRSM.SERVICE_CODE = :service_code  ORDER BY WR.ID DESC        ) WR        WHERE rownum < :p4    ) WR    WHERE r__ >= :p5) WR JOIN WUSER_RELATION_SERVICE_MAP WRSM ON WR.ACCOUNT_NUMBER = WRSM.ACCOUNT_NUMBER AND WR.ID = WRSM.USER_REL_ID WHERE WR.TYPE = :type2 AND WRSM.SERVICE_CODE = :service_code2"
  sqlStr4 = "SELECT /* JoinedMap.FIND_ACTIVE_PRIMARY_ACCOUNT_PARTY_RELATIONSHIP.1 */ AP.RELATIONSHIP, AP.STATUS, AP.ACCOUNT_NUMBER FROM ACCOUNT AP, ACCOUNT_PARTY_PRIMARY APP WHERE AP.ACCOUNT_NUMBER = :account_number AND AP.RELATIONSHIP = :relationship_type AND (APP.ACCOUNT_NUMBER = AP.ACCOUNT_NUMBER) ORDER BY AP.ID DESC"
  sqlStr5 = "SELECT /* JoinedMap.FIND_ACCOUNT_PARTY_RELATIONSHIPS.1 */ APP.UPDATE_VERSION FROM ACCOUNT_PARTY_PRIMARY APP, (    SELECT AP.RELATIONSHIP_TYPE, AP.UPDATE_VERSION FROM    (        SELECT AP.UPDATE_VERSION, rownum r__        FROM        (SELECT AP.UPDATE_VERSION FROM ACCOUNT_PARTY AP WHERE AP.ACCOUNT_NUMBER = :account_number AND AP.RELATIONSHIP_TYPE = :relationship_type ORDER BY AP.ID DESC        ) AP        WHERE rownum < :p3    ) AP    WHERE r__ >= :p4) AP WHERE (APP.ACCOUNT_NUMBER = AP.ACCOUNT_NUMBER)"
  sqlStr6 = "SELECT /* JoinedMap.FINDBYLAYERID.1 */ PCL.NAME, PCL.TYPE, PCL.ACTIVE_END_TIME, PCL.ACTIVE_START_TIME, PCL.ID, PCL.TIME_CREATED, PCL.TIME_UPDATED, PCL.UPDATE_VERSION, PCLP.PROD_CONFIG_PARAM_NAME, PCLP.VALUE, PCLP.PROD_CONFIG_LAYER_ID, PCLP.TIME_CREATED, PCLP.TIME_UPDATED, PCLP.UPDATE_VERSION, PCLPN.PROD_CONFIG_LAYER_NAME, PCLPN.PROD_CONFIG_LAYER_ID, PCLPN.TIME_CREATED FROM PROD_CONFIG_LAYER PCL LEFT JOIN ( PROD_CONFIG_LAYER_PARAM PCLP  ) ON PCL.ID = PCLP.PROD_CONFIG_LAYER_ID LEFT JOIN ( PROD_CONFIG_LAYER_PUBLIC_NAME PCLPN  ) ON PCL.ID = PCLPN.PROD_CONFIG_LAYER_ID WHERE PCL.ID = :id AND ((1 = 1))"
  sqlStr7 = "SELECT hello FROM DUAL"
)

func TestRemoveComments(t *testing.T) {
  t.Log("++++Running TestRemoveComments")
  output := removeComments(sqlStr)
  if strings.Compare(output, outStr) != 0 {
    t.Error("Incorrect output from TestRemoveComments:"+output)
  }
  t.Log("----Done TestRemoveComments")
}

func TestTableNameParser(t *testing.T) {
  t.Log("++++Running TestTableNameParser")
  output := TableNameParser(sqlStr)
  if strings.Compare(output[0], "PROD_CONFIG_LAYER_PUBLIC_NAME") != 0 {
    t.Error("Incorrect output from TestTableNameParser:"+output[0])
  }
  t.Log("----Done TestTableNameParser")
}

func TestTableNameParserWithJoinSQL(t *testing.T) {
  t.Log("++++Running TestTableNameParserWithJoinSQL")
  output := TableNameParser(sqlStr2)
  if strings.Compare(output[0], "table-name1") != 0 || strings.Compare(output[1], "table-name2") != 0 {
    t.Error("Incorrect output from TestTableNameParserWithJoinSQL:"+ output[0] + " " + output[1])
  }
  t.Log("----Done TestTableNameParserWithJoinSQL")
}

func TestTableNameParserWithJoinSQL2(t *testing.T) {
  t.Log("++++Running TestTableNameParserWithJoinSQL2")
  output := TableNameParser(sqlStr3)
  if strings.Compare(output[0], "WUSER_RELATIONSHIP") != 0 || strings.Compare(output[1], "WUSER_RELATION_SERVICE_MAP") != 0 {
    t.Error("Incorrect output from TestTableNameParserWithMultipleTables:"+ output[0] + " " + output[1])
  }
  t.Log("----Done TestTableNameParserWithJoinSQL2")
}

func TestTableNameParserWithMultipleTables(t *testing.T) {
  t.Log("++++Running TestTableNameParserWithMultipleTables")
  output := TableNameParser(sqlStr4)
  if strings.Compare(output[0], "ACCOUNT") != 0 || strings.Compare(output[1], "ACCOUNT_PARTY_PRIMARY") != 0 {
    t.Error("Incorrect output from TestTableNameParserWithMultipleTables:"+ output[0] + " " + output[1])
  }
  t.Log("----Done TestTableNameParserWithMultipleTables")
}

func TestTableNameParserWithMultipleTablesWithNestedQuery(t *testing.T) {
  t.Log("++++Running TestTableNameParserWithMultipleTablesWithNestedQuery")
  output := TableNameParser(sqlStr5)
  if strings.Compare(output[0], "ACCOUNT_PARTY_PRIMARY") != 0 || strings.Compare(output[1], "ACCOUNT_PARTY") != 0 {
    t.Error("Incorrect output from TestTableNameParserWithMultipleTablesWithNestedQuery:"+ output[0] + " " + output[1])
  }
  t.Log("----Done TestTableNameParserWithMultipleTablesWithNestedQuery")
}

func TestTableNameParserMultiJoinsQuery(t *testing.T) {
  t.Log("++++Running TestTableNameParserMultiJoinsQuery")
  output := TableNameParser(sqlStr6)
  printOutput(output)
  if strings.Compare(output[0], "PROD_CONFIG_LAYER") != 0 || strings.Compare(output[1], "PROD_CONFIG_LAYER_PARAM") != 0 || strings.Compare(output[2], "PROD_CONFIG_LAYER_PUBLIC_NAME") != 0{
    t.Error("Incorrect output from TestTableNameParserMultiJoinsQuery:"+ output[0] + " " + output[1] + " " + output[2])
  }
  t.Log("----Done TestTableNameParserMultiJoinsQuery")
}

func TestTableNameParserDualQuery(t *testing.T) {
  t.Log("++++Running TestTableNameParserDualQuery")
  output := TableNameParser(sqlStr7)
  if strings.Compare(output[0], "DUAL") != 0 {
    t.Error("Incorrect output from TestTableNameParserDualQuery:"+ output[0])
  }
  t.Log("----Done TestTableNameParserDualQuery")
}

func printOutput(output []string){
  for _, each := range output{
    fmt.Println(each)
  }
}
