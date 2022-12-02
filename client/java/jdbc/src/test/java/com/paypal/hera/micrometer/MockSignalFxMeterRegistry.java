package com.paypal.hera.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.step.StepCounter;
import io.micrometer.signalfx.SignalFxConfig;
import io.micrometer.signalfx.SignalFxMeterRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockSignalFxMeterRegistry extends SignalFxMeterRegistry {
    public MockSignalFxMeterRegistry(SignalFxConfig config, Clock clock) {
        super(config, clock);
    }

    private Map<String, ArrayList<MeterInfoTest>> meterInfoMap = new HashMap<>();

    @Override
    protected void publish() {
        List<Meter> meters = this.getMeters();

        for (Meter meter : meters) {
            String name = meter.getId().getName();
            String db_host = meter.getId().getTag("db_host");
            String sql_hash = meter.getId().getTag("sql_hash");
            String az = meter.getId().getTag("az");
            String env = meter.getId().getTag("environment");
            String app = meter.getId().getTag("application");
            if (meter instanceof StepCounter) {
                StepCounter counter = (StepCounter) meter;
                double count = counter.count();

                ArrayList<MeterInfoTest> meterInfoList;
                if (meterInfoMap.containsKey(name)){
                    meterInfoList = meterInfoMap.get(name);
                }
                else {
                    meterInfoList = new ArrayList<>();
                }

                MeterInfoTest meterInfo;
                if (count > 0){
                    meterInfo = new MeterInfoTest(name, db_host, sql_hash, count);
                    meterInfoList.add(meterInfo);
                    meterInfoMap.put(name, meterInfoList);
                }



            }
            else if (meter instanceof Timer){
                Timer timer = (Timer) meter;
                Map<String, Double> timeInfo = new HashMap<>();
                double totalTime = timer.totalTime(TimeUnit.SECONDS);
                double max = timer.max(TimeUnit.SECONDS);
                if (totalTime > 0 && max > 0){
                    timeInfo.put("totalTime", totalTime);
                    timeInfo.put("max", max);

                    ArrayList<MeterInfoTest> meterInfoList;
                    MeterInfoTest meterInfo = new MeterInfoTest(name, db_host, sql_hash, timeInfo);
                    if (meterInfoMap.containsKey(name)){
                        meterInfoList = meterInfoMap.get(name);
                    }
                    else {
                        meterInfoList = new ArrayList<>();
                    }
                    meterInfoList.add(meterInfo);
                    meterInfoMap.put(name, meterInfoList);
                }
            }

        }
        super.publish();
    }

    public Map<String, ArrayList<MeterInfoTest>> getMeterInfoMap() {
        return meterInfoMap;
    }

    public void cleanUp(){
        meterInfoMap = new HashMap<>();
    }
}
