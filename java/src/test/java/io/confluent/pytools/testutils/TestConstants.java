/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.pytools.testutils;

import java.time.Duration;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestConstants {
  public static class TIMEOUTS {

    public static final Integer LATCH_TIMEOUT_SECONDS = 120;
    public static final Integer DEFAULT_TIMEOUT_SECONDS = 15;
    public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS);
    public static final Duration CONSUME_AWAIT_TIMEOUT = Duration.ofSeconds(180);
    public static final Duration CONNECT_STOP_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration POLL_INTERVAL = Duration.ofMillis(100);
  }
}
