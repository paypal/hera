package com.paypal.hera.jdbc;

import com.paypal.hera.constants.HeraJdbcDriverConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HeraJdbcDriverConstantsTest {
    @Test
    public void testInitialisation() {
        HeraJdbcDriverConstants constants = HeraJdbcDriverConstants.getInstance();

        List<String> expectedListOfErrorMessages = new ArrayList<>();

        expectedListOfErrorMessages.add("HERA error: HERA-100: backlog timeout");
        expectedListOfErrorMessages.add("HERA error: HERA-102: backlog eviction");
        expectedListOfErrorMessages.add("HERA error: HERA-103: request rejected, database down");
        expectedListOfErrorMessages.add("HERA error: HERA-104: saturation soft sql eviction");
        expectedListOfErrorMessages.add("OCC error: HERA-105: bind throttle");
        expectedListOfErrorMessages.add("OCC error: HERA-106: bind eviction");
        expectedListOfErrorMessages.add("Unexpected end of stream");

        for(String expected : expectedListOfErrorMessages) {
            Assert.assertTrue(expected, constants.s_staleConnErrors.contains(expected));
        }
    }
}
