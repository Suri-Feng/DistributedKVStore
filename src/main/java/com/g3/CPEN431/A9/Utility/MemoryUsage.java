package com.g3.CPEN431.A9.Utility;

public class MemoryUsage {
    private static final long MEGABYTE = 1024L * 1024L;
    public static long getMaxMemory() {
        return bytesToMegabytes(Runtime.getRuntime().maxMemory());
    }

    public static long getUsedMemory() {
        return bytesToMegabytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public static long getFreeMemory() {
        return getMaxMemory() - getUsedMemory();
    }

    private static long bytesToMegabytes (long bytes) {
        return bytes / MEGABYTE;
    }
}