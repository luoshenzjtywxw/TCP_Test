/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.HashMap;
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
    // 超时定时器（只对 sendBase 包计时）
    private UDT_Timer timer = null;
    private UDT_RetransTask retransTask = null;
    private TCP_PACKET ackPack = null;
    /*构造函数*/
    public TCP_Receiver() {
        super();
        super.initTCP_Receiver(this);
    }

    @Override
    public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();

        // 检查校验和
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            System.out.println("Corrupted packet! Sending cumulative ACK anyway.");
            // 损坏包也要发送累计确认
//            sendCumulativeAck(recvPack.getSourceAddr());
            return;
        }

        // 检查是否在接收窗口内 [expectedSeq, expectedSeq + WINDOW_SIZE)
        if (isInWindow(recvSeq, ackSeq, WINDOW_SIZE)) {
            // 缓存数据（即使乱序）
            dataBuf.put(recvSeq, recvPack.getTcpS().getData());
            received.put(recvSeq, true);

            System.out.println("收到次序为=" + recvSeq+"的包");

            // 先 deliver 再发 ACK（确保 ACK 反映最新状态）
            // 比如收到了序号为5的包，然后发送的是5，期望变成了6，下一次收到序号为6的包，然后发送期望为6的ACK，而不是7，如果没收到序号为6的包，则发送的ACK为6，而不是5，因为代表了期望的包
            // 接收者收到了这个6，就会接着发送6的包，并代表之前的已经确认了（因为之前没有确认，接收者的期望也不会变成6）
            if(recvSeq == ackSeq){
                deliverInOrder();
                // 开启定时器

            }

            sendCumulativeAck(recvPack.getSourceAddr());



        } else {

            System.out.println("接收者收到在窗口外的包：" + recvSeq);
            System.out.println("目前窗口位置为：("+ ackSeq +"-"+(ackSeq +WINDOW_SIZE)+")");
            // 仍发送当前累积 ACK，这里也要发！
            sendCumulativeAck(recvPack.getSourceAddr());
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


    private void sendCumulativeAck(InetAddress destAddr) {
        int ackNum = ackSeq-1; // TCP 标准：ACK = 下一个期望序号
        System.out.println("发送累积确认值为"+ ackNum +"的ACK");

        tcpH.setTh_ack(ackNum);
        TCP_PACKET ackPack = new TCP_PACKET(tcpH, tcpS, destAddr);
        tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
        reply(ackPack);
    }
    private void startTimer() {
        if (timer != null) {
            timer.cancel();
        }
        timer = new UDT_Timer();
        // 更新一下包：
        if(ackPack == null){
            ackPack = new TCP_PACKET(tcpH, tcpS);
        }
        retransTask = new UDT_RetransTask(client, ackPack);
        timer.schedule(retransTask, 3000); // 一次性超时（Reno 通常单次）
        System.out.println("Started timer for base seq=" + sendBase);
    }
}