package com.paypal.dal.heramockclient.mockannotation;

import com.ebay.kernel.cal.api.CalTransaction;
import com.ebay.kernel.cal.api.sync.CalTransactionFactory;
import com.ebay.kernel.cal.api.sync.CalTransactionHelper;
import com.paypal.dal.heramockclient.HERAMockAction;
import com.paypal.dal.heramockclient.HERAMockHelper;
import com.paypal.infra.util.cal.CorrelationId;

import java.io.IOException;
import java.lang.reflect.Method;

public class HERAMockImpl {

    private CalTransaction calTransaction = null;
    private boolean calTransactionCreatedLocally = false;
    private boolean isConnectionLevelMock = true;
    private boolean isInCaptureMode = false;
    private String captureFileName = "";
    private String mockKey = "";

    private void init(){
        isInCaptureMode = false;
        isConnectionLevelMock = true;
    }

    public void enableHERAMock(Object object, String methodName) throws NoSuchMethodException, IOException{
        init();
        if (object == null || methodName == null) {
            throw new NoSuchMethodException("Method Name or class given for mocking is NULL - please check enableHERAMock call");
        }
        calTransaction = CalTransactionHelper.getTopTransaction();
        Method method = object.getClass().getMethod(methodName);
        HERAMock om = method.getAnnotation(HERAMock.class);
        if (om != null) {
            mockKey = om.MockId();
            if (calTransaction == null) {
                calTransaction = CalTransactionFactory.createRootTransaction("API");
                calTransactionCreatedLocally = true;
                calTransaction.setStatus("0");
                calTransaction.setName(object.getClass().getSimpleName() + "_" + method.getName());
            }

            if(calTransaction.getCorrelationId() == null || calTransaction.getCorrelationId().equals("NotSet"))
                calTransaction.setCorrelationId(CorrelationId.getNextId());

            calTransaction.addData("_corrid", calTransaction.getCorrelationId());
            calTransaction.addData("HERAMockAction", om.MockAction());

            isConnectionLevelMock = true;
            if (mockKey.length() == 0) {
                mockKey = calTransaction.getCorrelationId();
            }
            if (om.MockAction().equals(HERAMockAction.CONNECTION_TIMEOUT)) {
                mockKey = om.MockId();
                HERAMockHelper.simulateConnectionTimeout(om.MockId());
            } else if (om.MockAction().equals(HERAMockAction.SIMULATE_AUTH_FAILURE)) {
                mockKey = om.MockId();
                HERAMockHelper.simulateCustomAuthConnectionFailure(om.MockId());
            } else if (om.MockAction().equals(HERAMockAction.SIMULATE_CLOCK_SKEW)) {
                mockKey = om.MockId();
                HERAMockHelper.simulateConnectionClockSkew(om.MockId());
            } else if (om.MockAction().equals(HERAMockAction.CAPTURE)) {
                HERAMockHelper.startRRCapture(mockKey);
                isInCaptureMode = true;
                captureFileName = om.CaptureFile();
                if (captureFileName.equals("")) {
                    captureFileName = methodName + ".dal.hera.log";
                }
            } else if (om.MockAction().equals(HERAMockAction.REPLAY)) {
                HERAMockHelper.replayCaptured(mockKey, om.CaptureFile());
            } else{
                isConnectionLevelMock = false;
                HERAMockHelper.addMock(mockKey, om.MockAction(), om.NthOccurrence(), om.expireTimeInSec(), 0);
            }

            CalTransaction calTransaction = CalTransactionHelper.getTopTransaction();
            if(calTransaction != null)
                System.out.println(calTransaction.getCorrelationId());
        }
    }

    public void cleanHERAMock(){
        CalTransaction calTransaction = CalTransactionHelper.getTopTransaction();
        if(calTransaction != null)
            System.out.println(calTransaction.getCorrelationId());
        if(mockKey != null && isConnectionLevelMock) {
            HERAMockHelper.removeConnectionMock(mockKey);
        }
        else if(mockKey != null) {
            HERAMockHelper.removeMock(mockKey);
        }
        if (calTransaction != null &&
                calTransaction.getCorrelationId() != null) {
            try {
                if (isInCaptureMode)
                    HERAMockHelper.endRRCapture(calTransaction.getCorrelationId(), captureFileName);
            } catch (IOException ex){
                System.out.println("unable to stop capture " + ex.getMessage() + " current path: " +
                        System.getProperty("user.dir"));
            }
            if(calTransactionCreatedLocally) {
                calTransaction.completed();
                calTransactionCreatedLocally = false;
            }
        }
    }
}



