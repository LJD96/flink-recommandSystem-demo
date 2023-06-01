package com.demo.sink;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RecentOneHourRedisSink implements RedisMapper<Long> {


    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET, null);
    }

    @Override
    public String getKeyFromData(Long num) {
        return "recent_one_hour";
    }

    @Override
    public String getValueFromData(Long num) {
        return String.valueOf(num);
    }
}
