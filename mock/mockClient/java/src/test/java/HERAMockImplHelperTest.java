import com.paypal.hera.heramockclient.*;
import com.paypal.hera.parser.QueryParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HERAMockImplHelperTest {

    @Test
    public void testListMock() {
        Map<String, String> map = HERAMockHelper.listMock();
        for (String key : map.keySet())
            System.out.println(key + ": " + map.get(key));
        Assert.assertTrue("Mock Size cannot be empty", map.size() > 0);
    }

    @Test
    public void testSetServerIp() {
        String previousIP = HERAMockHelper.getServerIp();
        Assert.assertTrue("Failed to set server ip for mock", HERAMockHelper.setServerIp("test"));
        Assert.assertEquals("test", HERAMockHelper.getServerIp());
        Assert.assertTrue("Failed to revert the Server IP", HERAMockHelper.revertServerIp());
        Assert.assertEquals(previousIP, HERAMockHelper.getServerIp());
    }

    @Test
    public void testSetMock() {
        Assert.assertTrue("Failed to set Mock", HERAMockHelper.addMock("test", "value"));
        Assert.assertEquals("value", HERAMockHelper.getMock("test"));
        Assert.assertTrue("Failed to remove Mock", HERAMockHelper.removeMock("test"));
        Assert.assertNotEquals("value", HERAMockHelper.getMock("test"));
        HERAMockHelper.setMockIP("127.0.0.1");
        Assert.assertEquals("127.0.0.1", HERAMockHelper.getMockIP());
    }

    @Test
    public void testReloadMock() {
        Assert.assertTrue("Failed to Reload Mock", HERAMockHelper.reloadMock());
    }

    @Test
    public void testConnectionMock() {
        Assert.assertTrue("Failed to set custom auth failure",
                HERAMockHelper.simulateCustomAuthConnectionFailure());
        Assert.assertEquals("1005 simulating auth failure", HERAMockHelper.getMock("accept"));
        Assert.assertTrue("Failed to set connect timeout failure",
                HERAMockHelper.simulateConnectionTimeout());
        Assert.assertEquals(HERAMockAction.CONNECTION_TIMEOUT, HERAMockHelper.getMock("accept"));
        Assert.assertTrue("Failed to set clock skew failure",
                HERAMockHelper.simulateConnectionClockSkew());
        Assert.assertEquals("1010 simulating clockskew", HERAMockHelper.getMock("accept"));
        Assert.assertTrue("Failed to set custom auth failure", HERAMockHelper.removeConnectionMock());
        Assert.assertNotEquals("1010 simulating clockskew", HERAMockHelper.getMock("accept"));
    }

    @Test
    public void testAddMockKeySpace() {
        String key ="10101" + HERAMockAction.ADD_MOCK_CONSTRAINT +
                "HibernateR1FailoverTest_testQueryTimeout_1_1719891628";
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.addMock(key, "test key"));
        Assert.assertTrue("Failed to remove mock", HERAMockHelper.removeMock(key));

        Assert.assertNotEquals("test key", HERAMockHelper.getMock(key));
    }

    @Test
    public void testKeyValueEqual() {
        String key ="10101=";
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.addMock(key, "test=key"));
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.getMock(key).equals("test=key"));
        Assert.assertTrue("Failed to remove mock", HERAMockHelper.removeMock(key));

        Assert.assertNotEquals("test=key", HERAMockHelper.getMock(key));
    }

    @Test
    public void testKeyValueUnaryAnd() {
        String key ="&10101&";
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.addMock(key, "&test&key"));
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.getMock(key).equals("&test&key"));
        Assert.assertTrue("Failed to remove mock", HERAMockHelper.removeMock(key));

        Assert.assertNotEquals("&test&key", HERAMockHelper.getMock(key));

        key ="port=10101&service=paymentserv";
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.addMock(key, "key=value&payload=make"));
        Assert.assertTrue("Failed to add Mock", HERAMockHelper.getMock(key).equals("key=value&payload=make"));
        Assert.assertTrue("Failed to remove mock", HERAMockHelper.removeMock(key));

        Assert.assertNotEquals("key=value&payload=make", HERAMockHelper.getMock(key));
    }

    @Test
    public void testSeqMock() throws HERAMockException {
//        14:0 3:3 1,3:3 0,,
//        44:0 3:3 1,9:3 NEXTVAL,3:3 2,3:3 0,3:3 0,3:3 0,,
//        12:3 4295221574,
//        1,6
        NextValMock seq = new NextValMock();
        seq.setNextval(4295221574L);
        String out = HERAMockHelper.getObjectMock(seq, false, 0);
        System.out.println(out);
        String sql = "select /* ABCQuery */ ABC.nextval from dual";
        String resp = QueryParser.getMockMetaForQuery(sql, out);
        System.out.println(resp);
    }

    @Test
    public void testMockLog() {
        List<MockLog> mockLogList = HERAMockHelper.mockLogs();
        for(MockLog log : mockLogList)
            System.out.println(log.toString());
    }

    @Test
    public void testMockLogFilter() {
        List<MockLog> mockLogList = HERAMockHelper.mockLogs("EMailMap._PrimaryKeyLookup.-2");
        for(MockLog log : mockLogList)
            System.out.println(log.toString());
    }
}
