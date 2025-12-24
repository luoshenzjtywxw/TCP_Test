/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

        // 检查校验和
        if (CheckSum.computeChkSum(recvPack) != recvPack.getTcpH().getTh_sum()) {
            // 数据包损坏 → 发送 NAK
            System.out.println("Packet corrupted! Sending NAK.");
            tcpH.setTh_ack(-1);
            ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));

            //回复ACK报文段
            reply(ackPack);
            return;
        }
        int recvSeq = recvPack.getTcpH().getTh_seq();

        if (recvSeq == sequence) {
            // 正确且按序
            dataQueue.offer(recvPack.getTcpS().getData());
            System.out.println("Deliver data with seq=" + recvSeq);

            tcpH.setTh_ack(sequence);
            ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
            reply(ackPack);

            // 切换期望序号
            sequence = 1 - sequence;

        } else {
            // 重复包（比如重传的旧包）
            System.out.println("Duplicate packet (seq=" + recvSeq + "), sending ACK for last good seq.");
            // 发送对上一个包的 ACK（即 1 - expectedSeq）
            tcpH.setTh_ack(1-sequence);
            ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
            tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
            reply(ackPack);
            // 注意：不交付数据！
        }

//		//检查校验码，生成ACK
//		if(CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum()) {
//			//生成ACK报文段（设置确认号）
//			tcpH.setTh_ack(recvPack.getTcpH().getTh_seq());
//			ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
//			tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
//			//回复ACK报文段
//			reply(ackPack);
//
//			//将接收到的正确有序的数据插入data队列，准备交付
//			dataQueue.add(recvPack.getTcpS().getData());
//			sequence++;
//		}else{
//			System.out.println("Recieve Computed: "+CheckSum.computeChkSum(recvPack));
//			System.out.println("Recieved Packet"+recvPack.getTcpH().getTh_sum());
//			System.out.println("Problem: Packet Number: "+recvPack.getTcpH().getTh_seq()+" + InnerSeq:  "+sequence);
//			tcpH.setTh_ack(-1);
//			ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
//			tcpH.setTh_sum(CheckSum.computeChkSum(ackPack));
//			//回复ACK报文段
//			reply(ackPack);
//		}

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
	
}
