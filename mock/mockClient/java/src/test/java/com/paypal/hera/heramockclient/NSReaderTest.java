package com.paypal.hera.heramockclient;

import com.paypal.hera.heramockclient.NSReader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;

public class NSReaderTest {

    private JSONObject callRequestParser(String requestString) throws Exception{
        NSReader nsReader = new NSReader();
        JSONObject resp = nsReader.parseRequest(requestString);
        Iterator iterator = resp.keys();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            System.out.println(key + ": " + resp.get(key));
        }
        return resp;
    }

    private JSONObject callResponseParser(JSONArray responseList, JSONObject requestObject)
            throws Exception{
        NSReader nsReader = new NSReader();
        JSONObject resp = nsReader.parseResponse(responseList, requestObject, new HashMap<>());
        System.out.println(resp.toString(4));
        return resp;
    }

    @Test
    public void testBasicRequestRead() throws Exception{
        String requestString = "298:25 SELECT /* Account.FIND_BY_ACCOUNT_NUMBER */LAST_TIME_CLOSED, TIME_CREATED, TIME_UPDATED, LAST_TIME_OPENED, DISPOSITION, ACCOUNT_NUMBER_ENC, NAME, UPDATE_VERSION, PP_LEGAL_ENTITY, SCUTTLE_ID, REQUIRED_SECURITY_LEVEL, ACCOUNT_NUMBER, COUNTRY_CODE FROM ACCOUNT where ACCOUNT_NUMBER= :account_number,16:2 account_number,21:3 1152927840554865227,1:4,2:22,3:7 0,,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(requestString, new NSReader().reverseRequest(resp));
    }

    @Test
    public void testFetchBiggerValue() throws Exception
    {
        String requestString = "618:25 SELECT /* AcctBankAccountEventMap.FIND_ALL_BY_ACCOUNT_PIT_LIMIT.-2 */ OUTER.* FROM (SELECT RID FROM ( SELECT ROWID RID FROM acct_bank_account_event_01 WHERE ACCOUNT_NUMBER = :account_number AND DATE_EVENT_RECORDED <= :p2  AND TIME_EVENT_PUBLISHED <= :p3 ORDER BY TIME_EVENT_PUBLISHED ASC ) WHERE ROWNUM <= 500 UNION SELECT RID FROM ( SELECT ROWID RID FROM acct_bank_account_event_01 WHERE ACCOUNT_NUMBER = :account_number2 AND DATE_EVENT_RECORDED <= :p5  AND TIME_EVENT_PUBLISHED <= :p6 ORDER BY time_event_published DESC) WHERE ROWNUM <= 500 ) INNER, acct_bank_account_event_01 OUTER WHERE OUTER.ROWID = INNER.RID ,16:2 account_number,21:3 1296203123236105501,4:2 p2,4:10 6,25:3 03-12-2021 09:27:24.134,4:2 p3,15:3 1638552447187,17:2 account_number2,21:3 1296203123236105501,4:2 p5,4:10 6,25:3 03-12-2021 09:27:24.134,4:2 p6,15:3 1638552447187,1:4,5:7 100,,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(requestString, new NSReader().reverseRequest(resp));
    }

    @Test
    public void testFetchNoValue() throws Exception
    {
        String requestString = "618:25 SELECT /* AcctBankAccountEventMap.FIND_ALL_BY_ACCOUNT_PIT_LIMIT.-2 */ OUTER.* FROM (SELECT RID FROM ( SELECT ROWID RID FROM acct_bank_account_event_01 WHERE ACCOUNT_NUMBER = :account_number AND DATE_EVENT_RECORDED <= :p2  AND TIME_EVENT_PUBLISHED <= :p3 ORDER BY TIME_EVENT_PUBLISHED ASC ) WHERE ROWNUM <= 500 UNION SELECT RID FROM ( SELECT ROWID RID FROM acct_bank_account_event_01 WHERE ACCOUNT_NUMBER = :account_number2 AND DATE_EVENT_RECORDED <= :p5  AND TIME_EVENT_PUBLISHED <= :p6 ORDER BY time_event_published DESC) WHERE ROWNUM <= 500 ) INNER, acct_bank_account_event_01 OUTER WHERE OUTER.ROWID = INNER.RID ,16:2 account_number,21:3 1296203123236105501,4:2 p2,4:10 6,25:3 03-12-2021 09:27:24.134,4:2 p3,15:3 1638552447187,17:2 account_number2,21:3 1296203123236105501,4:2 p5,4:10 6,25:3 03-12-2021 09:27:24.134,4:2 p6,15:3 1638552447187,1:4,1:7,,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(requestString, new NSReader().reverseRequest(resp));
    }

    @Test
    public void testSecondReadRequest() throws Exception {
        String requestString = "298:25 SELECT /* Account.FIND_BY_ACCOUNT_NUMBER */LAST_TIME_CLOSED, TIME_CREATED, TIME_UPDATED, LAST_TIME_OPENED, DISPOSITION, ACCOUNT_NUMBER_ENC, NAME, UPDATE_VERSION, PP_LEGAL_ENTITY, SCUTTLE_ID, REQUIRED_SECURITY_LEVEL, ACCOUNT_NUMBER, COUNTRY_CODE FROM ACCOUNT where ACCOUNT_NUMBER= :account_number,16:2 account_number,21:3 1152927840554865227,1:4,3:7 0,,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(new NSReader().reverseRequest(resp), requestString);
    }

    @Test
    public void testUpdateRequest() throws Exception {
        String requestString = "161:25 UPDATE /* Account.UPDATE_BY_ACCOUNT_NUMBER */ ACCOUNT SET TIME_UPDATED = :time_updated, UPDATE_VERSION = :update_version where ACCOUNT_NUMBER= :account_number,14:2 time_updated,15:3 1594330250004,16:2 update_version,3:3 2,16:2 account_number,21:3 1152927840554865227,1:4,,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(new NSReader().reverseRequest(resp), requestString);
    }

    @Test
    public void testUpdateResponse() throws Exception {
        String requestString = "161:25 UPDATE /* Account.UPDATE_BY_ACCOUNT_NUMBER */ ACCOUNT SET TIME_UPDATED = :time_updated, UPDATE_VERSION = :update_version where ACCOUNT_NUMBER= :account_number,14:2 time_updated,15:3 1594330250004,16:2 update_version,3:3 2,16:2 account_number,21:3 1152927840554865227,1:4,,";
        JSONObject requestObject = callRequestParser(requestString);
        JSONArray responseList = new JSONArray();
        responseList.put("0 3:3 0,3:3 1,,");
        JSONObject resp = callResponseParser(responseList, requestObject);
        Assert.assertEquals(responseList.toString(), new NSReader().reverseResponse(resp, requestObject).toString());
    }

    @Test
    public void testCommitRequest() throws Exception {
        String requestString = "8,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(new NSReader().reverseRequest(resp), requestString);
    }

    @Test
    public void testRollbackRequest() throws Exception {
        String requestString = "9,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(requestString, new NSReader().reverseRequest(resp));
    }

    @Test
    public void testFetchRequest() throws Exception {
        String requestString = "7 100,";
        JSONObject resp = callRequestParser(requestString);
        Assert.assertEquals(requestString, new NSReader().reverseRequest(resp));
    }

    @Test
    public void testSelectResponse() throws Exception {
        JSONArray responseList = new JSONArray();
        responseList.put("0 4:3 13,3:3 0,,");
        responseList.put("0 4:3 13,18:3 LAST_TIME_CLOSED,3:3 2,3:3 0,3:3 0,5:3 129,14:3 TIME_CREATED,3:3 2,3:3 0,3:3 0,5:3 129,14:3 TIME_UPDATED,3:3 2,3:3 0,3:3 0,5:3 129,18:3 LAST_TIME_OPENED,3:3 2,3:3 0,3:3 0,5:3 129,13:3 DISPOSITION,3:3 1,3:3 8,3:3 0,3:3 0,20:3 ACCOUNT_NUMBER_ENC,3:3 1,4:3 32,3:3 0,3:3 0,6:3 NAME,3:3 1,4:3 64,3:3 0,3:3 0,16:3 UPDATE_VERSION,3:3 2,3:3 0,3:3 0,5:3 129,17:3 PP_LEGAL_ENTITY,3:3 1,3:3 8,3:3 0,3:3 0,12:3 SCUTTLE_ID,3:3 2,3:3 0,3:3 0,5:3 129,25:3 REQUIRED_SECURITY_LEVEL,3:3 2,3:3 0,3:3 0,5:3 129,16:3 ACCOUNT_NUMBER,3:3 2,3:3 0,3:3 0,5:3 129,14:3 COUNTRY_CODE,3:3 1,3:3 4,3:3 0,3:3 0,,");
        responseList.put("0 3:3 0,12:3 1247712446,15:3 1594670824798,12:3 1247712446,6:3 OPEN,1:3,1:3,4:3 12,3:3 P,5:3 237,3:3 0,21:3 1152927840554865227,4:3 RU,,");
        responseList.put("6,");
        String requestString = "298:25 SELECT /* Account.FIND_BY_ACCOUNT_NUMBER */LAST_TIME_CLOSED, TIME_CREATED, TIME_UPDATED, LAST_TIME_OPENED, DISPOSITION, ACCOUNT_NUMBER_ENC, NAME, UPDATE_VERSION, PP_LEGAL_ENTITY, SCUTTLE_ID, REQUIRED_SECURITY_LEVEL, ACCOUNT_NUMBER, COUNTRY_CODE FROM ACCOUNT where ACCOUNT_NUMBER= :account_number,16:2 account_number,21:3 1152927840554865227,1:4,2:22,3:7 0,,";
//        JSONObject requestObject = callRequestParser(requestString);

//        JSONObject resp = callResponseParser(responseList, requestObject);
//        Assert.assertEquals(responseList.toString(), new NSReader().reverseResponse(resp, requestObject).toString());

        requestString = "298:25 SELECT /* Account.FIND_BY_ACCOUNT_NUMBER */LAST_TIME_CLOSED, TIME_CREATED, TIME_UPDATED, LAST_TIME_OPENED, DISPOSITION, ACCOUNT_NUMBER_ENC, NAME, UPDATE_VERSION, PP_LEGAL_ENTITY, SCUTTLE_ID, REQUIRED_SECURITY_LEVEL, ACCOUNT_NUMBER, COUNTRY_CODE FROM ACCOUNT where ACCOUNT_NUMBER= :account_number,16:2 account_number,21:3 1152927840554865227,1:4,3:7 0,,";
        responseList = new JSONArray();
        responseList.put("0 4:3 13,3:3 0,,");
        responseList.put("0 3:3 0,12:3 1247712446,15:3 1594757600769,12:3 1247712446,6:3 OPEN,1:3,1:3,4:3 31,3:3 P,5:3 237,3:3 0,21:3 1152927840554865227,4:3 RU,,");
        responseList.put("6,");
        JSONObject requestObject = callRequestParser(requestString);
        JSONObject resp = callResponseParser(responseList, requestObject);
        Assert.assertEquals(responseList.toString(), new NSReader().reverseResponse(resp, requestObject).toString());
    }

    @Test
    public void testCommitResponse() throws Exception {
        String requestString = "8,";
        JSONObject requestObject = callRequestParser(requestString);
        JSONArray responseList = new JSONArray();
        responseList.put("5,");
        JSONObject resp = callResponseParser(responseList, requestObject);
        Assert.assertEquals(responseList.toString(), new NSReader().reverseResponse(resp, requestObject).toString());
    }

    @Test
    public void testRollbackResponse() throws Exception {
        String requestString = "9,";
        JSONObject requestObject = callRequestParser(requestString);
        JSONArray responseList = new JSONArray();
        responseList.put("5,");
        JSONObject resp = callResponseParser(responseList, requestObject);
        Assert.assertEquals(responseList.toString(), new NSReader().reverseResponse(resp, requestObject).toString());
    }
}
