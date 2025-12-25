/***************************2.1: ACK/NACK
 **************************** Feng Hong; 2015-12-09*/

package com.ouc.tcp.test;

import com.ouc.tcp.client.TCP_Sender_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TCP_Sender extends TCP_Sender_ADT {
    private static final int WINDOW_SIZE = 4;        // 发送窗口大小
    private static final int MAX_SEQ = 100;            // 序号范围 0~7
    private final BlockingQueue<Integer> ackQueue = new LinkedBlockingQueue<>();
    // 缓存已发送但未确认的包
    private final Map<Integer, TCP_PACKET> sndBuf = new HashMap<>();
    // 每个序号对应的定时器和重传任务
    private final Map<Integer, UDT_Timer> timers = new HashMap<>();
    private final Map<Integer, UDT_RetransTask> retransTasks = new HashMap<>();
    private int ackNum;
    private TCP_PACKET tcpPack;    //待发送的TCP数据报
    private int base = 0;                            // 窗口左边界（最早未确认序号）
    private int nextseqnum = 0;                      // 下一个要发送的序号


    /*构造函数*/
    public TCP_Sender() {
        super();    //调用超类构造函数
        super.initTCP_Sender(this);        //初始化TCP发送端
    }

    @Override
    //可靠发送（应用层调用）：封装应用层数据，产生TCP数据报；需要修改
    public void rdt_send(int dataIndex, int[] appData) {
// 如果窗口已满，阻塞等待（简化处理：直接返回或 busy-wait）

        while (nextseqnum >= base + WINDOW_SIZE) {
            try {
                Thread.sleep(10); // 简单等待
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        tcpH = new TCP_HEADER(); // 或从模板克隆
        tcpH.setTh_seq(nextseqnum);

        tcpS = new TCP_SEGMENT();
        tcpS.setData(appData);
        tcpPack = new TCP_PACKET(tcpH, tcpS, destinAddr);
        tcpH.setTh_sum(CheckSum.computeChkSum(tcpPack));

        // 发送
        udt_send(tcpPack);

        // 缓存已发送单位确认的包
        sndBuf.put(nextseqnum, tcpPack);

        // 启动独立定时器
        startTimer(nextseqnum, tcpPack);

        nextseqnum = (nextseqnum + 1) % MAX_SEQ;

        // 注意：不再调用 waitACK() —— ACK 处理在后台线程中进行
    }

    private void startTimer(int seq, TCP_PACKET packet) {
        System.out.println("在启动前取消了seq=" + seq + "的定时器");
        System.out.println("启动了seq=" + seq + "的定时器");
        cancelTimer(seq); // 先取消旧的（防重复）
        UDT_Timer timer = new UDT_Timer();
        UDT_RetransTask task = new UDT_RetransTask(client, packet);
        timer.schedule(task, 200, 200); // 只执行一次（SR 通常单次超时重传）

        timers.put(seq, timer);
        retransTasks.put(seq, task);
    }

    private void cancelTimer(int seq) {
        UDT_Timer timer = timers.remove(seq);
        if (timer != null) {
            timer.cancel();
        }
        retransTasks.remove(seq);
    }

    @Override
    //不可靠发送：将打包好的TCP数据报通过不可靠传输信道发送；仅需修改错误标志
    public void udt_send(TCP_PACKET stcpPack) {
        //设置错误控制标志
        tcpH.setTh_eflag((byte) 7);
        System.out.println("发送方发送了 " + stcpPack.getTcpH().getTh_seq() + "的包");
        //发送数据报
        client.send(stcpPack);
    }

    @Override
    //需要修改
    public void waitACK() {
        // 检查是否在 [base, base + WINDOW_SIZE) 范围内（模运算）
        if (isInWindow(ackNum, base, WINDOW_SIZE)) {
            System.out.println("在窗口中，现在窗口大小是从" + base + "到" + (base + WINDOW_SIZE) % 8);
            // 取消该分组的定时器
            cancelTimer(ackNum);
            System.out.println("在waitAck中取消" + ackNum + "的定时器");
            // 标记为已确认（从缓存移除）
            if (sndBuf.remove(ackNum) == null) {
                System.out.println("该序号的包已确认，却还在尝试移除");
            } else {
                System.out.println("该序号的包第一次确认");
            }

            // 如果是 base，尝试滑动窗口
            if (ackNum == base) {
                // 向右滑动窗口：找到最小的未确认序号
                int newBase = base;
                // 即到达发送了但未确认的第一个包
                while (!sndBuf.containsKey(newBase)) {
                    newBase = (newBase + 1) % MAX_SEQ;
                    if (newBase == nextseqnum) break; // 全部确认
                }
                base = newBase;
                System.out.println("Window slid: base = " + base);
                System.out.println();
            } else {
                System.out.println("Not base, no sliding.");
            }
        } else {
            System.out.println("Out-of-window ACK: " + ackNum);
            System.out.println();
        }
    }

    @Override
    //接收到ACK报文：检查校验和，将确认号插入ack队列;NACK的确认号为－1；不需要修改
    public void recv(TCP_PACKET recvPack) {
        // 校验 ACK 包
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            System.out.println("Corrupted ACK! Discarded.");
            return;
        }

        ackNum = recvPack.getTcpH().getTh_ack();
        System.out.println("Received ACK for seq=" + ackNum);


        // 直接处理 ACK（更高效）
        waitACK();
    }

    // 判断 seq 是否在 [start, start + size) 环形窗口内
    private boolean isInWindow(int seq, int start, int size) {
        // 第一个条件不可能达成
        if (size >= MAX_SEQ) return true;

        if (start + size <= MAX_SEQ) {
            // 例如start=1，seq=4，size=4
            // 那么4>=1&&4<1+4
            return seq >= start && seq < start + size;
        } else {
            // 环绕情况
            return seq >= start || seq < (start + size) % MAX_SEQ;
        }
    }

}
