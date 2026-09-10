package com.paycore.common.clock;

import java.time.Clock;
import java.time.Instant;

/**
 * Injectable clock abstraction so time-sensitive flows (idempotency lease expiry,
 * stuck AUTHORIZING payment resolver threshold, webhook replay window) can be
 * deterministically controlled in tests without Thread.sleep.
 */
@FunctionalInterface
public interface ClockProvider {

    Instant now();

    static ClockProvider systemUtc() {
        return () -> Clock.systemUTC().instant();
    }

    static ClockProvider fixed(Instant fixedInstant) {
        return () -> fixedInstant;
    }
}
