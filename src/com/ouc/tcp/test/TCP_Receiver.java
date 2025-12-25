/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Receiver extends TCP_Receiver_ADT {
    private static final int WINDOW_SIZE = 4;
    private static final int MAX_SEQ = 8;

    private int expectedSeq = 0; // 期望的下一个按序包
    // 缓存：记录哪些序号已收到（true/false）
    private final boolean[] received = new boolean[MAX_SEQ];
    // 数据缓存
    private final int[][] dataBuf = new int[MAX_SEQ][];

    private final BlockingQueue<int[]> dataQueue = new LinkedBlockingQueue<>();
	/*构造函数*/
	public TCP_Receiver() {
		super();	//调用超类构造函数
		super.initTCP_Receiver(this);	//初始化TCP接收端
	}

	@Override
	//接收到数据报：检查校验和，设置回复的ACK报文段
	public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();
        // 检查校验和
        // 损害了就不管了，等着发送方超时
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            // 发送最后一个按序确认的 ACK
//            sendAck((expectedSeq - 1 + MAX_SEQ) % MAX_SEQ, recvPack.getSourceAddr());
            return;
        }

        // 检查是否在接收窗口内 [expectedSeq, expectedSeq + WINDOW_SIZE)
        if (isInWindow(recvSeq, expectedSeq, WINDOW_SIZE)) {
            // 缓存数据（即使乱序）
            // 这个好像是如果在窗口内的重复数据，就会覆盖，但没关系
            dataBuf[recvSeq] = recvPack.getTcpS().getData();
            // 标记为已收到
            received[recvSeq] = true;

            System.out.println("Cached packet with seq=" + recvSeq);

            // 立即发送 ACK（无论是否按序）
            sendAck(recvSeq, recvPack.getSourceAddr());

            // 尝试交付连续数据
            deliverInOrder();
        } else {
            // 包在窗口外，说明接收者收到了，但是发送确认出问题了，那么在确认下，不然发送者会一直重发
            System.out.println("Out-of-window packet: " + recvSeq);
            sendAck(recvSeq, recvPack.getSourceAddr()); // 或者发 expectedSeq-1?
        }

		System.out.println();
		
		
		//交付数据（每20组数据交付一次）
		if(dataQueue.size() == 20) 
			deliver_data();	
	}
    private void deliverInOrder() {
        while (received[expectedSeq]) {
            // 提交数据
            dataQueue.offer(dataBuf[expectedSeq]);
            System.out.println("Delivered in-order data: seq=" + expectedSeq);

            // 清空缓存
            received[expectedSeq] = false;
            dataBuf[expectedSeq] = null;

            // 推进期望序号
            expectedSeq = (expectedSeq + 1) % MAX_SEQ;
        }
    }
    private boolean isInWindow(int seq, int start, int size) {
        if (size >= MAX_SEQ) return true;
        if (start + size <= MAX_SEQ) {
            return seq >= start && seq < start + size;
        } else {
            return seq >= start || seq < (start + size) % MAX_SEQ;
        }
    }
	@Override
	//交付数据（将数据写入文件）；不需要修改
	public void deliver_data() {
		//检查dataQueue，将数据写入文件
		File fw = new File("recvData.txt");
		BufferedWriter writer;
		
		try {
			writer = new BufferedWriter(new FileWriter(fw, true));
			
			//循环检查data队列中是否有新交付数据
			while(!dataQueue.isEmpty()) {
				int[] data = dataQueue.poll();
				
				//将数据写入文件
				for(int i = 0; i < data.length; i++) {
					writer.write(data[i] + "\n");
				}
				
				writer.flush();		//清空输出缓存
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	//回复ACK报文段
	public void reply(TCP_PACKET replyPack) {
		//设置错误控制标志
		tcpH.setTh_eflag((byte)4);	//eFlag=0，信道无错误
				
		//发送数据报
		client.send(replyPack);
	}
    // 辅助方法：发送 ACK
    private void sendAck(int ackNum, InetAddress destAddr) {
        tcpH.setTh_ack(ackNum);
        TCP_PACKET ackPack = new TCP_PACKET(tcpH, tcpS, destAddr);
        tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
        reply(ackPack);
    }
}
