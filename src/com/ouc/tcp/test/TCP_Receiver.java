/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;

public class TCP_Receiver extends TCP_Receiver_ADT {
    private static final int WINDOW_SIZE = 500;

    private int ackSeq = 0; // 期望的下一个按序包（自然增长，不取模）

    // 使用 Map 替代固定数组：动态支持任意 seq
    private final Map<Integer, Boolean> received = new ConcurrentHashMap<>();
    private final Map<Integer, int[]> dataBuf = new ConcurrentHashMap<>();
    // 一个线程内访问，同时也是线程安全的
    private final BlockingQueue<int[]> dataQueue = new LinkedBlockingQueue<>();
    // 超时定时器
    private UDT_Timer timer = null;
    private UDT_RetransTask retransTask;
    private InetAddress sourceAddr;
    private TCP_PACKET tcpPack;

    private long lastRecvTime = System.currentTimeMillis(); // 最后一次收到合法包的时间
    private UDT_Timer idleTimer = null;                    // 空闲检测定时器
    private static final long IDLE_TIMEOUT = 4000;          // 空闲超时阈值（500ms）

    /*构造函数*/
    public TCP_Receiver() {
        super();
        super.initTCP_Receiver(this);
    }

    @Override
    public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();
        lastRecvTime = System.currentTimeMillis();
        resetIdleTimer();

        // 检查校验和
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            System.out.println("Corrupted packet! Sending cumulative ACK anyway.");
            // 损坏包也要发送累计确认
            startTimerAndReply();
            return;
        }
        sourceAddr = recvPack.getSourceAddr();
        startTimerNotReply();
        // 检查是否在接收窗口内 [expectedSeq, expectedSeq + WINDOW_SIZE)
        if (isInWindow(recvSeq, ackSeq, WINDOW_SIZE)) {
            // 缓存数据（即使乱序）
            dataBuf.put(recvSeq, recvPack.getTcpS().getData());
            received.put(recvSeq, true);

            System.out.println("收到次序为=" + recvSeq+"的包");

            // 先 deliver 再发 ACK（确保 ACK 反映最新状态）
            // 比如收到了序号为5的包，然后发送的是5，期望变成了6，下一次收到序号为6的包，然后发送期望为6的ACK，而不是7，如果没收到序号为6的包，则发送的ACK为6，而不是5，因为代表了期望的包
            // 接收者收到了这个6，就会接着发送6的包，并代表之前的已经确认了（因为之前没有确认，接收者的期望也不会变成6）
            deliverInOrder();

            tcpH.setTh_ack(ackSeq-1);
            tcpH.setTh_sum(CheckSum.computeChkSum(tcpPack));

        } else {

            System.out.println("接收者收到在窗口外的包：" + recvSeq);
            System.out.println("目前窗口位置为：("+ ackSeq +"-"+(ackSeq +WINDOW_SIZE)+")");
            // 仍发送当前累积 ACK，这里也要发！
            startTimerAndReply();
        }

        System.out.println();

        // 每20组交付一次
        if (dataQueue.size() >= 20)
            deliver_data();
    }

    private void deliverInOrder() {
        // 只要 expectedSeq 已收到，就持续交付

        while (Boolean.TRUE.equals(received.get(ackSeq))) {
            int[] data = dataBuf.get(ackSeq);
            if (data != null) {
                dataQueue.offer(data);
//                System.out.println("Delivered in-order data: seq=" + expectedSeq);
            }

            // 清理缓存（可选，节省内存）
            received.remove(ackSeq);
            dataBuf.remove(ackSeq);

            ackSeq++; // 自然递增，不取模！
        }
    }

    // 简化：因为 expectedSeq 不会环绕，直接数值判断
    private boolean isInWindow(int seq, int start, int size) {
        return seq >= start && seq < start + size;
    }

    @Override
    public void deliver_data() {
        File fw = new File("recvData.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fw, true))) {
            while (!dataQueue.isEmpty()) {
                int[] data = dataQueue.poll();
                for (int value : data) {
                    writer.write(value + "\n");
                }
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reply(TCP_PACKET replyPack) {
        tcpH.setTh_eflag((byte) 7); // 无错误
        client.send(replyPack);
    }
    private void startTimerAndReply() {
        if (timer != null) {
            timer.cancel();
        }
        // 发送
        reply(tcpPack);
        timer = new UDT_Timer();
        retransTask = new UDT_RetransTask(client, tcpPack) ;
        timer.schedule(retransTask, 500,500); // 一次性超时（Reno 通常单次）
    }
    private void startTimerNotReply() {
        if (timer == null) {
            tcpH.setTh_ack(ackSeq - 1);
            tcpPack = new TCP_PACKET(tcpH, tcpS, sourceAddr);
            tcpH.setTh_sum(CheckSum.computeChkSum(tcpPack));
            // 发送
//        reply(tcpPack);
            timer = new UDT_Timer();
            retransTask = new UDT_RetransTask(client, tcpPack);
            timer.schedule(retransTask, 500, 500); // 一次性超时（Reno 通常单次）
        }
    }
    private void resetIdleTimer() {
        if (idleTimer != null) {
            idleTimer.cancel();
        }
        idleTimer = new UDT_Timer();
        idleTimer.schedule(new java.util.TimerTask() {
            @Override
            public void run() {
                checkIdleTimeout();
            }
        }, IDLE_TIMEOUT); // 500ms 后检查
    }
    private void checkIdleTimeout() {
        long now = System.currentTimeMillis();
        if (now - lastRecvTime >= IDLE_TIMEOUT) {
            // 确认空闲：停止所有定时器，完成最后交付
            System.out.println("检测到 4s 无新包，认为传输结束。");

            // 停止周期性 ACK 定时器
            if (timer != null) {
                timer.cancel();
                timer = null;
            }

            // 停止空闲定时器（自己）
            idleTimer.cancel();
            idleTimer = null;

            // 交付剩余数据（即使不足20）
            deliver_data();

            // 可选：通知客户端关闭？或设置标志位
            // client.shutdown(); // 如果有此类接口

        } else {
            // 还没超时，可能是被提前触发（比如 reset 后旧 timer 还在）
            // 安全起见，再设一次（或忽略）
            resetIdleTimer(); // 或者不做任何事，因为 reset 已经覆盖
        }
    }
}