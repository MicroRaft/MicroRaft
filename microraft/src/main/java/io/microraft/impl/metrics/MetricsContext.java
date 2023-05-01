package io.microraft.impl.metrics;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import edu.emory.mathcs.backport.java.util.Collections;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.microraft.RaftEndpoint;

public class MetricsContext {

    public static final String RAFT_GROUP_ID_TAG_NAME = "groupId";

    public static final String RAFT_ENDPOINT_TAG_NAME = "endpoint";

    private final Clock clock;

    private final MeterRegistry meterRegistry;

    private final List<Tag> tags;

    public MetricsContext(Clock clock, MeterRegistry meterRegistry, Object groupId, RaftEndpoint endpoint,
            List<Tag> extraTags) {
        this.clock = requireNonNull(clock);
        this.meterRegistry = requireNonNull(meterRegistry);
        List<Tag> tags = new ArrayList<>(2);
        tags.add(Tag.of(RAFT_GROUP_ID_TAG_NAME, requireNonNull(groupId).toString()));
        tags.add(Tag.of(RAFT_ENDPOINT_TAG_NAME, requireNonNull(endpoint).getId().toString()));
        if (extraTags != null) {
            tags.addAll(extraTags);
        }
        this.tags = Collections.unmodifiableList(tags);
    }

    public MetricsContext(Clock clock, MeterRegistry meterRegistry, Object groupId, RaftEndpoint endpoint) {
        this(clock, meterRegistry, groupId, endpoint, Collections.emptyList());
    }

    public Clock clock() {
        return clock;
    }

    public MeterRegistry meterRegistry() {
        return meterRegistry;
    }

    public List<Tag> tags() {
        return tags;
    }

    public long nowMs() {
        return clock.millis();
    }

    public long elapsedDurationMs(long tsMs) {
        if (tsMs <= 0) {
            return 0;
        }

        return Math.max(nowMs() - tsMs, 0);
    }

    public Counter registerCounter(String name) {
        return Counter.builder(name).tags(tags).register(meterRegistry);
    }

    public Timer registerTimer(String name) {
        return Timer.builder(name).publishPercentiles(0.5, 0.95, 0.99, 1).publishPercentileHistogram().tags(tags)
                .register(meterRegistry);
    }

    public Gauge registerGauge(String name, Supplier<Number> supplierFunc) {
        return Gauge.builder(name, supplierFunc).tags(tags).register(meterRegistry);
    }

    public Gauge registerBooleanGauge(String name, Supplier<Boolean> supplierFunc) {
        return Gauge.builder(name, () -> (supplierFunc.get() ? 1 : 0)).tags(tags).register(meterRegistry);
    }

    public TimeGauge registerTimeGauge(String name, TimeUnit unit, Supplier<Number> supplierFunc) {
        return TimeGauge.builder(name, supplierFunc, TimeUnit.MILLISECONDS).tags(tags).register(meterRegistry);
    }

    public TimeGauge registerElapsedTimeGauge(String name, TimeUnit unit, Supplier<Number> supplierFunc) {
        return TimeGauge.builder(name, () -> (Math.max(nowMs() - (long) supplierFunc.get(), 0)), TimeUnit.MILLISECONDS)
                .tags(tags).register(meterRegistry);
    }

}
