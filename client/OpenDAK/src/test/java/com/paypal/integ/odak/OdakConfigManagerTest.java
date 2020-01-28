package com.paypal.integ.odak;

import com.ebay.kernel.cal.api.CalEvent;
import com.ebay.kernel.cal.api.sync.CalEventFactory;
import com.paypal.integ.odak.OdakConfigManager;
import com.paypal.platform.security.PayPalSSLHelper;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by alwjoseph on 12/21/18.
 */
public class OdakConfigManagerTest {

    @Test
    public void testIsODAKEnabled(){

        PayPalSSLHelper.initializeSecurity();
        boolean result = OdakConfigManager.getInstance().isODAKEnabled();
        System.out.println(result);
    }
    @Test
    public void testResolveWhitelistKeys(){

        PayPalSSLHelper.initializeSecurity();
        CalEvent calEvent = CalEventFactory.create("ResolveWhitelistKeys");
        Configuration configuration = new PropertiesConfiguration();
        configuration.addProperty(OdakConfigManager.RCS_KEY_WL_ODAK_POOL_NAMES,"poolname1\\,poolname2");
        configuration.addProperty( OdakConfigManager.RCS_KEY_WL_ODAK_BOX_NAMES,"boxname1\\,boxname2");
        boolean result = OdakConfigManager.getInstance().resolveWhitelistKeys("DALCP",calEvent,configuration,",poolname1,", ",boxname1," );
        Assert.assertTrue("resolveWhitelistKeys==>", result);

    }


    @Test
    public void testHandleODAKWLRCSKeys(){

        CalEvent calEvent = CalEventFactory.create("HandleODAKWLRCSKeys");
        Configuration configuration = new PropertiesConfiguration();
        configuration.addProperty(OdakConfigManager.RCS_KEY_WL_ODAK_POOL_NAMES,"poolname1\\,poolname2");
        configuration.addProperty( OdakConfigManager.RCS_KEY_WL_ODAK_BOX_NAMES,"boxname1\\,boxname2");
        boolean enabledODAK =  OdakConfigManager.getInstance().handleODAKWLRCSKeys(calEvent,configuration,",poolname1,", ",boxname1,");
        Assert.assertTrue("handleODAKWLRCSKeys==>", enabledODAK);
    }

    @Test
    public void testHandleDCPWLRCSKeys(){
        CalEvent calEvent = CalEventFactory.create("HandleDCPWLRCSKeys");
        Configuration configuration = new PropertiesConfiguration();

        boolean result = OdakConfigManager.getInstance().handleDCPWLRCSKeys(calEvent,configuration,",poolname1,", ",boxname1,");

        Assert.assertTrue("handleODAKWLRCSKeys==>", result);
    }

    @Test
    public void testHandleDCPWLRCSKeysWithDCPKeys(){
        CalEvent calEvent = CalEventFactory.create("HandleDCPWLRCSKeys");
        Configuration configuration = new PropertiesConfiguration();
        configuration.addProperty(OdakConfigManager.RCS_KEY_WL_DCP_POOL_NAMES,"poolname1\\,poolname2");
        configuration.addProperty( OdakConfigManager.RCS_KEY_WL_DCP_BOX_NAMES,"boxname1\\,boxname2");

        boolean result = OdakConfigManager.getInstance().handleDCPWLRCSKeys(calEvent,configuration,",poolname1,", ",boxname1,");

        Assert.assertFalse("HandleDCPWLRCSKeysWithDCPKeys==>", result);
    }

    @Test
    public void testHandleDCPWLRCSKeysWithDCPKeysAndOnlyPoolSpecific(){
        CalEvent calEvent = CalEventFactory.create("HandleDCPWLRCSKeys");
        Configuration configuration = new PropertiesConfiguration();
        configuration.addProperty(OdakConfigManager.RCS_KEY_WL_DCP_POOL_NAMES,"poolname1\\,poolname2");
        configuration.addProperty( OdakConfigManager.RCS_KEY_WL_DCP_BOX_NAMES,"boxname2");

        boolean result = OdakConfigManager.getInstance().handleDCPWLRCSKeys(calEvent,configuration,",poolname1,", ",boxname1,");

        Assert.assertFalse("HandleDCPWLRCSKeysWithDCPKeys==>", result);
    }

    @Test
    public void testHandleDCPWLRCSKeysWithDCPKeysOnlyBoxSpecific(){
        CalEvent calEvent = CalEventFactory.create("HandleDCPWLRCSKeys");
        Configuration configuration = new PropertiesConfiguration();
        configuration.addProperty(OdakConfigManager.RCS_KEY_WL_DCP_POOL_NAMES,"poolname2");
        configuration.addProperty( OdakConfigManager.RCS_KEY_WL_DCP_BOX_NAMES,"boxname1\\,boxname2");

        boolean result = OdakConfigManager.getInstance().handleDCPWLRCSKeys(calEvent,configuration,",poolname1,", ",boxname1,");

        Assert.assertFalse("HandleDCPWLRCSKeysWithDCPKeys==>", result);
    }


    @Test
    public void testIsNonDevEnv(){
        boolean result = OdakConfigManager.getInstance().isNonDevEnv();
        Assert.assertFalse("NonDev==>", result);
    }

}
