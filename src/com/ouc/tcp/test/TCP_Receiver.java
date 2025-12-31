/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;

public class TCP_Receiver extends TCP_Receiver_ADT {
    private static final int WINDOW_SIZE = 500;
    private static final long DELAYED_ACK_TIMEOUT = 500; // 500ms
    // 使用 Map 替代固定数组：动态支持任意 seq
    private final Map<Integer, Boolean> received = new ConcurrentHashMap<>();
    private final Map<Integer, int[]> dataBuf = new ConcurrentHashMap<>();
    // 一个线程内访问，同时也是线程安全的
    private final BlockingQueue<int[]> dataQueue = new LinkedBlockingQueue<>();
    private int ackSeq = 0; // 期望的下一个按序包（自然增长，不取模）
    // 超时定时器
    private UDT_Timer delayedAckTimer = null;
    private UDT_RetransTask retransTask;
    private InetAddress sourceAddr;
    private TCP_PACKET ackPack;

    /*构造函数*/
    public TCP_Receiver() {
        super();
        super.initTCP_Receiver(this);
    }

    @Override
    public void rdt_recv(TCP_PACKET recvPack) {
        // 检查校验和
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            System.out.println("Corrupted packet! Sending cumulative ACK anyway.");
            // 防止第一个包就是损坏的，那么不能回复，因为地址可能都是错的，只能等超时重发了
            return;
        }
        sourceAddr = recvPack.getSourceAddr();
        int recvSeq = recvPack.getTcpH().getTh_seq();
        int oldAckSeq = ackSeq;
        // 2. 处理重复包（seq < ackSeq）
        if (recvSeq < ackSeq) {
            System.out.println("收到重复包: " + recvSeq + "，立即发送重复 ACK");
            sendImmediateCumulativeAck();
            return;
        }
        // 3. 检查是否在接收窗口内
        if (!isInWindow(recvSeq, ackSeq, WINDOW_SIZE)) {
            System.out.println("收到窗口外的包: " + recvSeq + "，立即发送当前累积 ACK");
            sendImmediateCumulativeAck();
            return;
        }
        // 检查是否在接收窗口内 [expectedSeq, expectedSeq + WINDOW_SIZE)
        // 缓存数据（即使乱序）
        // 4. 缓存数据
        dataBuf.put(recvSeq, recvPack.getTcpS().getData());
        received.put(recvSeq, true);

        System.out.println("收到次序为=" + recvSeq + "的包");
        // 5. 判断是窗口左沿（按序）还是乱序
        if (recvSeq == oldAckSeq) {
            // ✅ 收到窗口左沿包：尝试交付并调度延迟 ACK
            // ack开始变化了
            deliverInOrder();
            ackPack = new TCP_PACKET(tcpH, tcpS, sourceAddr);
            tcpH.setTh_ack(ackSeq - 1);
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
            if (ackSeq > oldAckSeq) {
                System.out.println("窗口推进，启动定时器");
                startTimer(); // 只有窗口推进才启动定时器
            }
        } else {
            // ✅ 乱序包（如期望5，收到6/7/8...）：立即发送重复 ACK（触发快重传）
            System.out.println("收到乱序包: " + recvSeq + "，立即发送重复 ACK");
            sendImmediateCumulativeAck();
        }
        // 先 deliver 再发 ACK（确保 ACK 反映最新状态）
        // 比如收到了序号为5的包，然后发送的是5，期望变成了6，下一次收到序号为6的包，然后发送期望为6的ACK，而不是7，如果没收到序号为6的包，则发送的ACK为6，而不是5，因为代表了期望的包
        // 接收者收到了这个6，就会接着发送6的包，并代表之前的已经确认了（因为之前没有确认，接收者的期望也不会变成6）
        // 可能定时器取消了（说明已经很长时间没有收到包了，发送完了），但是又有包过来了（发送端的最后一个包超时重发），就需要正常回应
        System.out.println();

        // 每20组交付一次
//        if (dataQueue.size() >= 20)
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

    // 立即发送当前累积 ACK（用于乱序包、重复包、窗口外包）
    private void sendImmediateCumulativeAck() {

        if (delayedAckTimer != null) {
            delayedAckTimer.cancel();
            delayedAckTimer = null;
        }
        sendCumulativeAck();
    }

    // 调度 500ms 延迟 ACK 定时器（用于累积确认）
    private void startTimer() {
        if (delayedAckTimer != null) {
            delayedAckTimer.cancel();
        }
        delayedAckTimer = new UDT_Timer();
        retransTask = new UDT_RetransTask(client, ackPack);
        delayedAckTimer.schedule(retransTask, DELAYED_ACK_TIMEOUT);
    }

    // 实际构造并发送 ACK 包
    private void sendCumulativeAck() {
        if (sourceAddr == null) return;
        ackPack = new TCP_PACKET(tcpH, tcpS, sourceAddr);
        tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
        reply(ackPack);
        System.out.println("发送累积 ACK: " + (ackSeq - 1));
    }
//    private void startTimerAndReply() {
//        if (timer != null) {
//            timer.cancel();
//        }
//
//        // 发送
//        reply(ackPack);
//        timer = new UDT_Timer();
//        retransTask = new UDT_RetransTask(client, ackPack);
//        timer.schedule(retransTask, 500, 500); // 一次性超时（Reno 通常单次）
//    }

//    private void startTimerNotReply() {
//        if (timer == null) {
//            tcpH.setTh_ack(ackSeq - 1);
//            ackPack = new TCP_PACKET(tcpH, tcpS, sourceAddr);
//            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
//            // 发送
////        reply(tcpPack);
//            timer = new UDT_Timer();
//            retransTask = new UDT_RetransTask(client, ackPack);
//            timer.schedule(retransTask, 500, 500); // 一次性超时（Reno 通常单次）
//        }
//    }
}