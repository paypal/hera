package com.paypal.integ.odak;

import com.paypal.hera.dal.cm.ConnectionPoolConfig;
import com.paypal.hera.dal.cm.wrapper.CmConnectionCallback;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapter;
import com.paypal.hera.dal.tests.BaseIntegrationTestCase;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

public class OdakPoolTest extends BaseIntegrationTestCase {

    @Test
    public void testOdakConnectionAdd() throws Exception{
        PoolConfig poolConfigMock = Mockito.mock(PoolConfig.class);
        JdbcDriverAdapter jdbcDriverAdapterMock = Mockito.mock(JdbcDriverAdapter.class);
        CmConnectionCallback cmConnectionCallbackMock = Mockito.mock(CmConnectionCallback.class);
        ConnectionPoolConfig connectionPoolConfigMock = Mockito.mock(ConnectionPoolConfig.class);

        String url = "jdbc:oracle:thin:@lvsvmdb74.qa.paypal.com:2126:QAMISC";
        OdakPool odakPool = new OdakPool("msmaster.qa.paypal.com", poolConfigMock, jdbcDriverAdapterMock,
                cmConnectionCallbackMock, connectionPoolConfigMock);

        Properties properties = new Properties();
        properties.setProperty("user", "pd_dla_kernal");
        properties.setProperty("password", "z1XgXYeb");

        Mockito.when(poolConfigMock.getMaxConnections()).thenReturn(1000);
        Mockito.when(connectionPoolConfigMock.getConnectionProperties()).thenReturn(properties);
        Mockito.when(connectionPoolConfigMock.getJdbcURL()).thenReturn(url);

        StateLogger.getInstance().register(odakPool);
        odakPool.processConnectRequest();
        Assert.assertNotNull(odakPool.getPooledConnection());
    }

    @Test
    public void testOdakConnectionSpike() throws Exception{
        PoolConfig poolConfigMock = Mockito.mock(PoolConfig.class);
        JdbcDriverAdapter jdbcDriverAdapterMock = Mockito.mock(JdbcDriverAdapter.class);
        CmConnectionCallback cmConnectionCallbackMock = Mockito.mock(CmConnectionCallback.class);
        ConnectionPoolConfig connectionPoolConfigMock = Mockito.mock(ConnectionPoolConfig.class);


        String url = "jdbc:oracle:thin:@lvsvmdb74.qa.paypal.com:2126:QAMISC";
        OdakPool odakPool = new OdakPool("msmaster.qa.paypal.com", poolConfigMock, jdbcDriverAdapterMock,
                cmConnectionCallbackMock, connectionPoolConfigMock);

        Properties properties = new Properties();
        properties.setProperty("user", "pd_dla_kernal");
        properties.setProperty("password", "z1XgXYeb");

        Mockito.when(poolConfigMock.getMaxConnections()).thenReturn(1000);
        Mockito.when(connectionPoolConfigMock.getConnectionProperties()).thenReturn(properties);
        Mockito.when(connectionPoolConfigMock.getJdbcURL()).thenReturn(url);

        StateLogger.getInstance().register(odakPool);
        odakPool.processConnectRequest();
        Assert.assertNotNull(odakPool.getPooledConnection());
        Thread.sleep(10000);
        Assert.assertNotNull(odakPool.getPooledConnection());
        odakPool.removeOrphanConnections();
    }
}
