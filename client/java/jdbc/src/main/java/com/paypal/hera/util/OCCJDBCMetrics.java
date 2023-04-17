package com.paypal.hera.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

public class OCCJDBCMetrics {

    String sqlHash;
    private Counter execFailCounter;
    private Counter execSuccessCounter;
    private Counter fetchFailCounter;
    private Counter fetchSuccessCounter;
    private Timer execTimer;
    private Timer fetchTimer;

    public OCCJDBCMetrics(String sqlHash) {
        this.sqlHash = sqlHash;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public Counter getExecFailCounter() {
        return execFailCounter;
    }

    public void setExecFailCounter(Counter execFailCounter) {
        this.execFailCounter = execFailCounter;
    }

    public Counter getExecSuccessCounter() {
        return execSuccessCounter;
    }

    public void setExecSuccessCounter(Counter execSuccessCounter) {
        this.execSuccessCounter = execSuccessCounter;
    }

    public Counter getFetchFailCounter() {
        return fetchFailCounter;
    }

    public void setFetchFailCounter(Counter fetchFailCounter) {
        this.fetchFailCounter = fetchFailCounter;
    }

    public Counter getFetchSuccessCounter() {
        return fetchSuccessCounter;
    }

    public void setFetchSuccessCounter(Counter fetchSuccessCounter) {
        this.fetchSuccessCounter = fetchSuccessCounter;
    }

    public Timer getExecTimer() {
        return execTimer;
    }

    public void setExecTimer(Timer execTimer) {
        this.execTimer = execTimer;
    }

    public Timer getFetchTimer() {
        return fetchTimer;
    }

    public void setFetchTimer(Timer fetchTimer) {
        this.fetchTimer = fetchTimer;
    }
}
