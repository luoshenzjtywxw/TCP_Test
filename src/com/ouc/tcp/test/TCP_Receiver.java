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
	
	private TCP_PACKET ackPack;	//回复的ACK报文段
	int sequence=0;//用于记录当前待接收的包序号，注意包序号不完全是
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
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            // ❗ 包损坏 → 不发 NAK，而是重发上一次的 ACK（即 1 - sequence）
            System.out.println("Packet corrupted! Sending duplicate ACK for seq=" + (1 - sequence));
            sendAck(1 - sequence, recvPack.getSourceAddr());
            return;
        }

        if (recvSeq == sequence) {
            // 正确且按序
            dataQueue.offer(recvPack.getTcpS().getData());
            System.out.println("Deliver data with seq=" + recvSeq);
            sendAck(sequence, recvPack.getSourceAddr());
            sequence = 1 - sequence; // 切换期望序号

        } else {
            // 重复包（比如重传的旧包）
            System.out.println("Duplicate packet (seq=" + recvSeq + "), sending duplicate ACK for seq=" + (1 - sequence));
            sendAck(1 - sequence, recvPack.getSourceAddr());
        }

		System.out.println();
		
		
		//交付数据（每20组数据交付一次）
		if(dataQueue.size() == 20) 
			deliver_data();	
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
		tcpH.setTh_eflag((byte)1);	//eFlag=0，信道无错误
				
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
