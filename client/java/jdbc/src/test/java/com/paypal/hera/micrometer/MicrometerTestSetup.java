package com.paypal.hera.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;

public class MicrometerTestSetup {
    private static MicrometerTestSetup setup = null;
    private static MockSignalFxMeterRegistry registry;

    private MicrometerTestSetup(){
        SfxConfig properties = new SfxConfig();
        registry = new MockSignalFxMeterRegistry(properties, Clock.SYSTEM);
        Metrics.globalRegistry.add(registry);
    }

    public static MockSignalFxMeterRegistry getRegistry(){
        return registry;
    }

    public static MicrometerTestSetup getInstance(){
        if (setup == null){
            setup = new MicrometerTestSetup();
        }

        return setup;
    }
}
