package com.paycore.common.ids;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RFC 9562 time-ordered UUIDv7 generator.
 *
 * <p>Layout (128 bits):
 * <ul>
 *   <li>48 bits: Unix epoch timestamp in milliseconds</li>
 *   <li>4 bits: Version (0111 = 7)</li>
 *   <li>12 bits: Monotonic counter within the same millisecond</li>
 *   <li>2 bits: Variant (10)</li>
 *   <li>62 bits: Cryptographically strong random bits</li>
 * </ul>
 *
 * <p>Why UUIDv7 (Section 5.1): B-tree primary keys in PostgreSQL suffer from random page splits
 * and buffer pool churn with UUIDv4. UUIDv7 preserves global uniqueness without coordination while
 * keeping inserts right-leaning in PostgreSQL indexes.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();
    // Packs [lastMillis (52 bits) | seq (12 bits)] for lock-free monotonic ordering within a millisecond
    private static final AtomicLong STATE = new AtomicLong(0L);

    private UuidV7() {}

    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    public static UUID generate(long epochMillis) {
        long seq;
        long timestampMillis;
        while (true) {
            long current = STATE.get();
            long lastMillis = current >>> 12;
            long lastSeq = current & 0xFFFL;

            if (epochMillis > lastMillis) {
                timestampMillis = epochMillis;
                seq = RANDOM.nextInt(256); // small random start per ms with headroom for 3840 monotonic increments
            } else {
                timestampMillis = lastMillis;
                seq = (lastSeq + 1) & 0xFFFL;
                if (seq == 0) {
                    // Counter overflow within the same ms: advance virtual millisecond by 1
                    timestampMillis = lastMillis + 1;
                }
            }

            long next = (timestampMillis << 12) | seq;
            if (STATE.compareAndSet(current, next)) {
                break;
            }
        }

        long msb = ((timestampMillis & 0xFFFFFFFFFFFFL) << 16)
                | 0x7000L // version 7
                | (seq & 0x0FFFL);

        long randomLong = RANDOM.nextLong();
        long lsb = (randomLong & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L; // RFC 9562 variant 2 (10xx)

        return new UUID(msb, lsb);
    }

    public static long extractEpochMillis(UUID uuid) {
        if (uuid.version() != 7) {
            throw new IllegalArgumentException("Not a UUIDv7: " + uuid);
        }
        return (uuid.getMostSignificantBits() >>> 16) & 0xFFFFFFFFFFFFL;
    }

    public static Instant extractInstant(UUID uuid) {
        return Instant.ofEpochMilli(extractEpochMillis(uuid));
    }
}
