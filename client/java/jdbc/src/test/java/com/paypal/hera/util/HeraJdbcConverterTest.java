package com.paypal.hera.util;

import org.junit.Assert;
import org.junit.Test;

public class HeraJdbcConverterTest {
    @Test
    public void testHera2Long(){
        byte[] val = "123456789".getBytes();
        long retVal = 123456789;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "0".getBytes();
        retVal = 0;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "-1".getBytes();
        retVal = -1;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "-123456789".getBytes();
        retVal = -123456789;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "9223372036854775807".getBytes();
        retVal = Long.MAX_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "-9223372036854775808".getBytes();
        retVal = Long.MIN_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));

        val = "-9223372036854775809".getBytes();
        try {
            Assert.assertEquals(retVal, HeraJdbcConverter.hera2long(val));
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("For input string: \"-9223372036854775809\""));
        }

        val = "9223372036854775808".getBytes();
        try {
            HeraJdbcConverter.hera2long(val);
            Assert.fail("should have thrown exception");
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("For input string: \"9223372036854775808\""));
        }
    }

    @Test
    public void testHera2int(){
        byte[] val = "123456789".getBytes();
        int retVal = 123456789;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "0".getBytes();
        retVal = 0;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "-1".getBytes();
        retVal = -1;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "-123456789".getBytes();
        retVal = -123456789;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "2147483647".getBytes();
        retVal = Integer.MAX_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "-2147483648".getBytes();
        retVal = Integer.MIN_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2int(val));

        val = "-2147483649".getBytes();
        try {
            HeraJdbcConverter.hera2int(val);
            Assert.fail("should have thrown exception");
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("For input string: \"-2147483649\""));
        }

        val = "2147483648".getBytes();
        try {
            HeraJdbcConverter.hera2int(val);
            Assert.fail("should have thrown exception");
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("For input string: \"2147483648\""));
        }
    }

    @Test
    public void testHera2Short(){
        byte[] val = "12345".getBytes();
        short retVal = 12345;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "0".getBytes();
        retVal = 0;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "-1".getBytes();
        retVal = -1;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "-12345".getBytes();
        retVal = -12345;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "32767".getBytes();
        retVal = Short.MAX_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "-32768".getBytes();
        retVal = Short.MIN_VALUE;
        Assert.assertEquals(retVal, HeraJdbcConverter.hera2short(val));

        val = "-32769".getBytes();
        try {
            HeraJdbcConverter.hera2short(val);
            Assert.fail("should have thrown exception");
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("Value out of range. Value:\"-32769\" Radix:10"));
        }

        val = "32768".getBytes();
        try {
            HeraJdbcConverter.hera2short(val);
            Assert.fail("should have thrown exception");
        }catch (NumberFormatException ex) {
            Assert.assertTrue(ex.getMessage(), ex.getMessage().equals("Value out of range. Value:\"32768\" Radix:10"));
        }
    }
}
