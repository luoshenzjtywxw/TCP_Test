// 文件路径: com/ouc/tcp/test/WindowLogger.java

package com.ouc.tcp.test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 用于记录 TCP 发送方窗口状态的日志工具类
 */
public class WindowLogger {
    private static PrintWriter writer = null;
    private static final String LOG_FILE = "tcp_sender_window.log";

    // 初始化日志文件（清空旧内容）
    public static void init() {
        try {
            // 确保目录存在
            Files.createDirectories(Paths.get("logs"));
            writer = new PrintWriter(new FileWriter("logs/" + LOG_FILE, false)); // false = 覆盖模式
            writer.println("# Time\tcwnd\tssthresh\tsendBase\tnextSeq\tdupAcks\tEvent");
            writer.flush();
        } catch (IOException e) {
            System.err.println("无法创建日志文件: " + e.getMessage());
        }
    }

    // 记录当前窗口状态
    public static synchronized void log(int cwnd, int ssthresh, int sendBase, int nextSeq, int dupAcks, String event) {
        if (writer == null) return;

        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        writer.printf("%s\t%d\t%d\t%d\t%d\t%d\t%s%n",
                time, cwnd, ssthresh, sendBase, nextSeq, dupAcks, event);
        writer.flush(); // 立即写入，避免丢失
    }

    // 关闭日志文件
    public static void close() {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}