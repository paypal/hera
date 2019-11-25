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
  sqlStr3 = "SELECT /* JoinedMap.IS_MAM_ACCOUNT_BY_SUBJECT_ACCOUNT.1 */ WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WR.ACCOUNT_NUMBER_ONE, WR.ACCOUNT_NUMBER_TWO, WR.ACTING_ACCOUNT, WR.FLAGS, WR.ID, WR.PYPL_TIME_TOUCHED, WR.SCUTTLE_ID, WR.TIME_CREATED, WR.UPDATE_VERSION, WRSM.STATUS, WRSM.SERVICE_CODE, WRSM.ACCOUNT_NUMBER, WRSM.FLAGS, WRSM.PYPL_TIME_TOUCHED, WRSM.SCUTTLE_ID, WRSM.TIME_CREATED, WRSM.UPDATE_VERSION, WRSM.USER_REL_ID FROM (    SELECT WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WR.ACCOUNT_NUMBER_ONE, WR.ACCOUNT_NUMBER_TWO, WR.ACTING_ACCOUNT, WR.FLAGS, WR.ID, WR.PYPL_TIME_TOUCHED, WR.SCUTTLE_ID, WR.TIME_CREATED, WR.UPDATE_VERSION FROM    (        SELECT WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WR.ACCOUNT_NUMBER_ONE, WR.ACCOUNT_NUMBER_TWO, WR.ACTING_ACCOUNT, WR.FLAGS, WR.ID, WR.PYPL_TIME_TOUCHED, WR.SCUTTLE_ID, WR.TIME_CREATED, WR.UPDATE_VERSION, rownum r__ FROM (SELECT /* JoinedMap.IS_MAM_ACCOUNT_BY_SUBJECT_ACCOUNT.1 */WR.STATUS, WR.TYPE, WR.ACCOUNT_NUMBER, WR.ACCOUNT_NUMBER_ONE, WR.ACCOUNT_NUMBER_TWO, WR.ACTING_ACCOUNT, WR.FLAGS, WR.ID, WR.PYPL_TIME_TOUCHED, WR.SCUTTLE_ID, WR.TIME_CREATED, WR.UPDATE_VERSION FROM WUSER_RELATIONSHIP WR JOIN WUSER_RELATION_SERVICE_MAP WRSM ON WR.ACCOUNT_NUMBER = WRSM.ACCOUNT_NUMBER AND WR.ID = WRSM.USER_REL_ID WHERE WR.ACCOUNT_NUMBER = :account_number AND WR.STATUS = 'A' AND WR.TYPE = :type AND WRSM.SERVICE_CODE = :service_code  ORDER BY WR.ID DESC        ) WR        WHERE rownum < :p4    ) WR    WHERE r__ >= :p5) WR JOIN WUSER_RELATION_SERVICE_MAP WRSM ON WR.ACCOUNT_NUMBER = WRSM.ACCOUNT_NUMBER AND WR.ID = WRSM.USER_REL_ID WHERE WR.TYPE = :type2 AND WRSM.SERVICE_CODE = :service_code2"
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
  t.Log("++++Running TestTableNameParserWithJoinSQL")
  output := TableNameParser(sqlStr3)
  for i := range output{
    fmt.Println(output[i])
  }
  t.Log("----Done TestTableNameParserWithJoinSQL")
}
