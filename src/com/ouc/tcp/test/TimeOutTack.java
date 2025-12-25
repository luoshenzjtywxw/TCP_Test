package com.ouc.tcp.test;

import com.ouc.tcp.client.Client;
import com.ouc.tcp.message.TCP_PACKET;

import java.util.TimerTask;

public class TimeOutTack extends TimerTask {
    private Client senderClient;
    private TCP_PACKET reTransPacket;

    public TimeOutTack(Client client, TCP_PACKET packet) {
        this.senderClient = client;
        this.reTransPacket = packet;
    }
    @Override
    public void run() {
        this.senderClient.send(this.reTransPacket);

    }

}
