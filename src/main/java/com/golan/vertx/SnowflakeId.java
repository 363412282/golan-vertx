package com.golan.vertx;

public class SnowflakeId {

    private final long epoch;  // 起始时间戳（可自定义）

    private final int timestampBits;
    private final int machineBits;
    private final int dataCenterBits;
    private final int sequenceBits;

    private final long maxDatacenterId;
    private final long maxMachineId;
    private final long maxSequence;

    private final long timestampShift;
    private final long dataCenterShift;
    private final long machineShift;

    private final long sequenceMask;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    private long dataCenterId;
    private long machineId;

    public SnowflakeId(long epoch, int timestampBits, int machineBits, int dataCenterBits, int sequenceBits, long dataCenterId, long machineId) {

        // 启始
        this.epoch = epoch;

        // 位数
        this.timestampBits = timestampBits;
        this.machineBits = machineBits;
        this.dataCenterBits = dataCenterBits;
        this.sequenceBits = sequenceBits;

        // 最大值
        this.maxDatacenterId = (1L << dataCenterBits) - 1;
        this.maxMachineId = (1L << machineBits) - 1;
        this.maxSequence = (1L << sequenceBits) - 1;

        // 位移
        this.timestampShift = sequenceBits + machineBits + dataCenterBits;
        this.dataCenterShift = sequenceBits + machineBits;
        this.machineShift = sequenceBits;

        // 序列号掩码
        this.sequenceMask = maxSequence;

        if (dataCenterId > maxDatacenterId || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("SnowflakeIdGenerator: Data Center Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        if (machineId > maxMachineId || machineId < 0) {
            throw new IllegalArgumentException(String.format("SnowflakeIdGenerator: Machine Id can't be greater than %d or less than 0", maxMachineId));
        }

        this.dataCenterId = dataCenterId;
        this.machineId = machineId;
    }

    public synchronized long nextId() {

        long timestamp = System.currentTimeMillis();

        if (timestamp == lastTimestamp) { // 如果时间戳与上次生成的时间戳相同，则根据序列号递增
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) { // 序列号溢出，等待下一毫秒
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0; // 重置序列号
        }

        if (timestamp < lastTimestamp) {
            throw new RuntimeException("Clock moved backwards. Refusing to generate id for " + (lastTimestamp - timestamp) + " milliseconds");
        }

        lastTimestamp = timestamp;

        // 移位并拼接各部分
        return ((timestamp - epoch) << timestampShift) | (dataCenterId << dataCenterShift) | (machineId << machineShift) | sequence;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    public long extractTimestamp(long id) {
        int shift = this.sequenceBits + this.machineBits + this.dataCenterBits;
        long timestampPart = id >> shift;
        return timestampPart + this.epoch;
    }
}
