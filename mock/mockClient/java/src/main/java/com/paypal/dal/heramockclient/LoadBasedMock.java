package com.paypal.dal.heramockclient;

public class LoadBasedMock {
    private int minRange;
    private int maxRange;
    private String successResponse;
    private String failureResponse;
    private int failurePercentage;
    private String key;

    public LoadBasedMock(String key, int minRange, int maxRange, int failurePercentage, String successResponse,
                         String failureResponse) {
        this.failurePercentage = failurePercentage;
        this.key = key;
        this.failureResponse = failureResponse;
        this.maxRange = maxRange;
        this.minRange = minRange;
        this.successResponse = successResponse;
    }

    public LoadBasedMock(String key, int minRange, int maxRange, int failurePercentage, Object successObject,
                         String failureResponse) throws Exception{
        this.failurePercentage = failurePercentage;
        this.failureResponse = failureResponse;
        this.key = key;
        this.maxRange = maxRange;
        this.minRange = minRange;
        this.successResponse = HERAMockHelper.getObjectMock(successObject, false, 0);
    }

    public String getKey() {
        return key;
    }

    public int getMinRange() {
        return minRange;
    }

    public int getMaxRange() {
        return maxRange;
    }

    public String getSuccessResponse() {
        return successResponse;
    }

    public String getFailureResponse() {
        return failureResponse;
    }

    public int getFailurePercentage() {
        return failurePercentage;
    }
}
