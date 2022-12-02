package com.paypal.hera.micrometer;

import io.micrometer.signalfx.SignalFxConfig;

import java.time.Duration;

public class SfxConfig implements SignalFxConfig {
    /**
     * @return {boolean} enable histogram
     */
    @Override
    public boolean publishCumulativeHistogram() {
        return true;
    }

    /**
     * @return {{@link String}} otel endpoint.
     */
    @Override
    public String uri() {
        return "https://otelmetrics-pp-observability.us-central1.gcp.dev.paypalinc.com:23081/v2/datapoint";
    }

    /**
     * @return {{@link String}} Signalfx access token
     */
    @Override
    public String accessToken() {
        return "";
    }

    /**
     * @return {{@link String}}
     */
    @Override
    public Duration step() {
        return Duration.ofSeconds(60);
    }

    /**
     * @return {@code true} if publishing is enabled. Default is {@code true}.
     */
    @Override
    public boolean enabled() {
        return true;
    }

    /**
     * Get the value associated with a key.
     *
     * @param key Key to lookup in the config.
     * @return Value for the key or null if no key is present.
     */
    @Override
    public String get(String key) {
        return null;
    }
}
