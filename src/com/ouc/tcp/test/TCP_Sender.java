package com.ouc.tcp.test;

import com.ouc.tcp.client.TCP_Sender_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;
import com.ouc.tcp.test.CheckSum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TCP_Sender extends TCP_Sender_ADT {
    // ===== 发送状态 =====
    // ✅ 移除了 MAX_SEQ！序号自然增长
    private final Map<Integer, TCP_PACKET> sndBuf = new ConcurrentHashMap<>();
    private double Rate = 0.0;
    // ===== Reno 拥塞控制参数 =====
    private int cwnd = 1;               // 拥塞窗口（包数）
    private int ssthresh = 65535;       // 慢启动阈值
    private int duplicateAcks = 0;      // 重复 ACK 计数
    private int nextSeq = 0;            // 下一个要发送的序号（自然递增）
    private int sendBase = 0;           // 最早未确认的序号（累积 ACK 基准）

    // 超时定时器（只对 sendBase 包计时）
    private UDT_Timer timer = null;
    private UDT_RetransTask retransTask = null;

    /*构造函数*/
    public TCP_Sender() {
        super();
        super.initTCP_Sender(this);
    }

    @Override
    public void rdt_send(int dataIndex, int[] appData) {
        // 阻塞直到窗口有空位（简单实现）
        while (nextSeq >= sendBase + cwnd) {
            System.out.println("窗口满了，目前大小为：" + cwnd + ", 部分发送序号为（" + nextSeq + "）的包");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        sendPacket(nextSeq, appData);
        System.out.println("应用层发送了序号为：" + nextSeq + "的包");
        nextSeq++;
    }

    private void sendPacket(int seq, int[] appData) {
        TCP_HEADER header = new TCP_HEADER();
        header.setTh_seq(seq);

        TCP_SEGMENT segment = new TCP_SEGMENT();
        segment.setData(appData);

        TCP_PACKET packet = new TCP_PACKET(header, segment, destinAddr);
        header.setTh_sum(CheckSum.computeChkSum(packet));

        udt_send(packet);

        // 如果是 sendBase（最老未确认包），启动定时器
        if (seq == sendBase) {
            startTimer(packet);
            System.out.println("在sendPacket中为序号为：" + seq + "的包启动定时器");
        }
        sndBuf.put(seq, packet);

    }

    private void startTimer(TCP_PACKET packet) {
        if (timer != null) {
            timer.cancel();
        }
        timer = new UDT_Timer();
        retransTask = new UDT_RetransTask(client, packet) {
            @Override
            public void run() {
                super.run(); // 执行重传
                handleTimeout(); // 触发拥塞控制
            }
        };
        timer.schedule(retransTask, 3000); // 一次性超时（Reno 通常单次）
        System.out.println("Started timer for base seq=" + sendBase);
    }

    private void stopTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
            retransTask = null;
            System.out.println("Stopped timer.");
        }
    }

    @Override
    public void udt_send(TCP_PACKET stcpPack) {
        stcpPack.getTcpH().setTh_eflag((byte) 7); // 无错误
        System.out.println("Sent packet seq=" + stcpPack.getTcpH().getTh_seq());
        client.send(stcpPack);
    }

    @Override
    public void recv(TCP_PACKET recvPack) {
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            System.out.println("Corrupted ACK! Discarded.");
            return;
        }

        int ack = recvPack.getTcpH().getTh_ack();
        System.out.println("Received ACK: " + ack);
        processAck(ack);
    }

    private void processAck(int ack) {
        if (ack >= sendBase) {
            // === 新 ACK（推进窗口）===
            duplicateAcks = 0;

            int oldBase = sendBase;
            // 这个不能改成sendBase = ack + 1;因为如果接受方没有收到包，发序号5所想要序号为5的包，你却直接把sendbase变成了6，那么你就不会传序号为5的包
            sendBase = ack+1;
            System.out.println("新的sendbase是" + sendBase);
            // 清除已确认的包
            for (int i = oldBase; i < sendBase; i++) {
                sndBuf.remove(i);
            }
            System.out.println("给序号为:" + oldBase + "的包取消定时");
            stopTimer();

            // 如果还有未确认包，为新的 sendBase 启动定时器
            if (sndBuf.containsKey(sendBase)) {
                // 我们要做一个处理，可能这是最后一个确认了
                System.out.println("在处理ACK中为序号为：" + sendBase + "的包启动定时器");
                startTimer(sndBuf.get(sendBase));
            }

            // === 拥塞控制 ===
            // 只有在还有未确认数据时才更新 cwnd（避免传输结束后无效增长）
            if (cwnd < ssthresh) {
                cwnd += 1;
                System.out.println("慢开始: cwnd=" + cwnd);
            } else {
                Rate += 1.0 / cwnd;
                if (Rate >= 1.0) {
                    cwnd += 1;
                    Rate -= 1.0;
                }
                System.out.println("拥塞避免: cwnd=" + cwnd);
            }

        } else if (ack == sendBase-1) {
            // === 重复 ACK ===
            duplicateAcks++;

            if (duplicateAcks == 3) {
                // Fast Retransmit
                System.out.println("收到三个重复ACK，快重传seq=" + sendBase);
                TCP_PACKET lost = sndBuf.get(sendBase);
                if (lost != null) {
                    udt_send(lost);
                    System.out.println("快重传了");
                }

                // Fast Recovery
                ssthresh = Math.max(cwnd / 2, 2);
                cwnd = ssthresh + 3;
                System.out.println("快恢复: ssthresh=" + ssthresh + ", cwnd=" + cwnd);

            } else if (duplicateAcks > 3) {
                // 在 Fast Recovery 中，每多一个 dup ACK，允许发送一个新包
                cwnd += 1;
                System.out.println("In Fast Recovery, extra dup ACK: cwnd=" + cwnd);
            }
        }
        // ack < sendBase: 忽略（过期 ACK）
    }

    public void handleTimeout() {
        System.out.println("超时了!重传序号为 seq=" + sendBase + "的包");
        duplicateAcks = 0;

        // 超时 → 慢启动
        ssthresh = Math.max(cwnd / 2, 2);
        cwnd = 1;

        // 重传由 UDT_RetransTask 完成，这里只更新状态
//        System.out.println("After timeout: ssthresh=" + ssthresh + ", cwnd=" + cwnd);
    }

    @Override
    public void waitACK() {
        // Reno 异步处理 ACK，不需要此方法
    }
}