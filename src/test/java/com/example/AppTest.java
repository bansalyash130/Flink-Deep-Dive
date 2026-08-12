package com.example;

import com.example.flink.FlinkJob;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link App}'s mode dispatch. {@link FlinkJob#run()} is mocked so it does
 * not block on the streaming pipeline. Only modes that do NOT start the Redis thread are
 * exercised here, to keep the tests off the network ("both"/"redis" would spawn a real
 * RedisDemo thread that mocked statics — being thread-local — cannot intercept).
 */
class AppTest {

    @Test
    void testFlinkModeInvokesFlinkJob() throws Exception {
        try (MockedStatic<FlinkJob> flink = mockStatic(FlinkJob.class)) {
            App.main(new String[]{"flink"});
            flink.verify(FlinkJob::run, times(1));
        }
    }

    @Test
    void testModeIsCaseInsensitive() throws Exception {
        try (MockedStatic<FlinkJob> flink = mockStatic(FlinkJob.class)) {
            App.main(new String[]{"FLINK"});
            flink.verify(FlinkJob::run, times(1));
        }
    }

    @Test
    void testUnknownModeInvokesNothing() throws Exception {
        try (MockedStatic<FlinkJob> flink = mockStatic(FlinkJob.class)) {
            App.main(new String[]{"nonsense"});
            flink.verify(FlinkJob::run, never());
        }
    }
}
