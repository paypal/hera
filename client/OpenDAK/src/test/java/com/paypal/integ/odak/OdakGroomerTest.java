package com.paypal.integ.odak;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.paypal.hera.dal.tests.BaseIntegrationTestCase;



public class OdakGroomerTest extends BaseIntegrationTestCase {

    @Test
    public void testOdakConnectionAdd() {
        OdakPool odakPoolMock = Mockito.mock(OdakPool.class);
        Mockito.when(odakPoolMock.getCurrentConnsCount()).thenReturn(10);
        Mockito.when(odakPoolMock.getSize()).thenReturn(20);
        OdakGroomer odakGroomer = OdakGroomer.getInstance();
        Assert.assertTrue(odakGroomer.addConnectionRequest(odakPoolMock, false));
    }
}
