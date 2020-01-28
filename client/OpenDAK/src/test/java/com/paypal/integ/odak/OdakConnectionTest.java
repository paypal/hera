package com.paypal.integ.odak;

import com.paypal.hera.dal.tests.BaseIntegrationTestCase;
import com.paypal.infra.occ.jdbc.OccConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

/**
 * Created by alwjoseph on 3/1/19.
 */
public class OdakConnectionTest extends BaseIntegrationTestCase {
    @Test
    public void testOdakConnectionCreationLocalMock() throws Exception {
        OccConnection occConnectionMock =  Mockito.mock(OccConnection.class);
        Mockito.when(occConnectionMock.getClientInfo(OccConnection.OCC_CLIENT_CONN_ID)).thenThrow( new SQLException());
        OdakPool odakPooMock = Mockito.mock(OdakPool.class);
        OdakConnection odakConnection = new OdakConnection(occConnectionMock,odakPooMock);
        Assert.assertNotNull(odakConnection);
    }

    @Test
   public void testToStringLocalMock(){
        OccConnection occConnectionMock =  Mockito.mock(OccConnection.class);
        OdakPool odakPooMock = Mockito.mock(OdakPool.class);
        OdakConnection odakConnection = new OdakConnection(occConnectionMock,odakPooMock);
        String toString = odakConnection.toString();
        Assert.assertNotNull(toString);
    }
}
