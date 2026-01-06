package com.ouc.tcp.test;

import com.ouc.tcp.message.TCP_HEADER;
import com.ouc.tcp.message.TCP_PACKET;

public class CheckSum {

    // 将32位int按大端顺序拆分为两个16位，并累加到checksum（使用反码求和）
    private static int add32AsTwo16(int checksum, int value) {
        // 高16位
        checksum += (value >>> 16) & 0xFFFF;
        // 低16位
        checksum += value & 0xFFFF;
        return checksum;
    }

    // 处理16位反码求和的进位（carry-around carry）
    private static int foldCarry(int sum) {
        while ((sum >> 16) != 0) {
            sum = (sum & 0xFFFF) + (sum >> 16);
        }
        return sum;
    }

    public static short computeChkSum(TCP_PACKET tcpPack) {
        int sum = 0;
        if(tcpPack==null){
            System.out.println("tcpPack is null");
        }
        TCP_HEADER header = tcpPack.getTcpH();

        // 1. 加入 seq（32位 → 两个16位）
        sum = add32AsTwo16(sum, header.getTh_seq());

        // 2. 加入 ack（32位 → 两个16位）
        sum = add32AsTwo16(sum, header.getTh_ack());

        // 3. 校验和字段本身设为0，所以跳过（不加）
        int[] data=null;
        if(tcpPack.getTcpS().getData()!=null){
            data = tcpPack.getTcpS().getData();
        }
        // 4. 加入数据部分（每个int视为4字节，按16位字处理）
        if (data != null) {
            for (int value : data) {
                // 每个int是4字节，拆成两个16位
                sum = add32AsTwo16(sum, value);
            }
        }

        // 5. 处理所有进位
        sum = foldCarry(sum);

        // 6. 取反，得到16位校验和
        sum = (~sum) & 0xFFFF;

        return (short) sum;
    }
}