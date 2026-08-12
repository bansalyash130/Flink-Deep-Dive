package com.example;

import com.example.flink.FlinkJob;
import com.example.redis.RedisDemo;

public class App {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0].toLowerCase() : "both"; // both | flink | redis

//         Redis runs on its own thread (Flink's execute() blocks the caller).
        if (mode.equals("both") || mode.equals("redis")) {
            new Thread(RedisDemo::run, "redis-demo").start();
        }

        // Flink runs on the main thread and blocks until the job is cancelled.
        if (mode.equals("both") || mode.equals("flink")) {
            FlinkJob.run();
        }
    }
}