package com.s42442146.CPEN431.A4.Utility;

public class MemoryUsage {
    private static final long MEGABYTE = 1024L * 1024L;
    public static long getMaxMemory() {
        return bytesToMegabytes(Runtime.getRuntime().maxMemory());
    }

    public static long getUsedMemory() {
        return bytesToMegabytes(getMaxMemory() - getFreeMemory());
    }

    public static long getTotalMemory() {
        return bytesToMegabytes(Runtime.getRuntime().totalMemory());
    }

    public static long getFreeMemory() {
        return bytesToMegabytes(Runtime.getRuntime().freeMemory());
    }

    private static long bytesToMegabytes (long bytes) {
        return bytes / MEGABYTE;
    }
}