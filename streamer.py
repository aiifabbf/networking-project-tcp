# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.seek = 0 # expect to receive body from

        self.sendBuffer = {} # key is ack number, value is body bytes
        self.seq = 0 # sending body from

        self.receiveBuffer = {} # key is sequence number, value is body bytes
        self.ack = 0 # expecting body from

        self.executor = concurrent.futures.ThreadPoolExecutor()

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        segmentSize = 1472
        headerSize = 8
        bodySize = segmentSize - headerSize

        for i in range(0, len(data_bytes), bodySize):
            body = data_bytes[i: i + bodySize]
            header = struct.pack(">ll", self.seq, self.ack)
            segment = header + body
            self.socket.sendto(segment, (self.dst_ip, self.dst_port))
            self.seq += len(body)
            self.sendBuffer[self.seq] = body

            self.asyncTraceSegment(self.seq)
            # self.executor.submit(self.asyncTraceSegment, self.seq)

    def asyncTraceSegment(self, ack: int) -> None:
        job = self.executor.submit(self.recvIntoBuffer)

        while True:
            time.sleep(0.25)
            if ack in self.sendBuffer:
                body = self.sendBuffer[ack]
                self.sendSegment(ack - len(body), self.ack, body)
                if job.done():
                    job = self.executor.submit(self.recvIntoBuffer)
            else:
                print("acked", ack)
                break

    def recvIntoBuffer(self) -> None:
        data, addr = self.socket.recvfrom()
        header = data[: 8]
        body = data[8: ]
        seq, ack = struct.unpack(">ll", header)
        # sender side
        if body: # a data + ack
            print("data segment", seq, ack, body, time.time())
            if seq == self.ack: # in order, normal situation
                self.receiveBuffer[seq] = body # put in buffer
                self.ack = seq + len(body) # expecting next body
                self.sendAck(self.seq, self.ack)
            elif seq > self.ack: # out of order, latter comes first
                self.receiveBuffer[seq] = body # put in buffer

                seek = self.ack
                complete = True

                while seek != seq:
                    if seek in self.receiveBuffer:
                        seek += len(self.receiveBuffer[seek])
                    else:
                        complete = False
                        break

                if complete:
                    self.ack = seq + len(body)
                    self.sendAck(self.seq, self.ack)
                else:
                    print("incomplete")
                    self.sendAck(self.seq, seek)
            else: # seq < self.ack
                self.sendAck(self.seq, self.ack)
        else: # an ack
            print("acked", seq, ack, time.time())
            pass

        # receiver side
        if ack == self.seq: # receiver gets all, normal situation
            self.sendBuffer.clear()
        elif ack < self.seq: # receiver gets partial, retransmit un-acked data

            toPop = []

            for k, v in self.sendBuffer.items():
                # if k - len(v) >= ack:
                #     self.sendSegment(k - len(v), self.ack, v)
                # else:
                #     toPop.append(k)
                if k <= ack:
                    toPop.append(k)
                else: # no need to retransmit here, because traceSegment() will take care of it
                    pass

            for k in toPop:
                self.sendBuffer.pop(k)

        else: # ack > self.seq not possible
            self.sendBuffer.clear()

    def sendAck(self, seq: int, ack: int) -> None:
        self.sendSegment(seq, ack)

    def sendSegment(self, seq: int, ack: int, body: bytes=b"") -> None:
        print("sent", seq, ack, body, time.time())
        header = struct.pack(">ll", seq, ack)
        segment = header + body
        self.socket.sendto(segment, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        while True:
            if self.seek in self.receiveBuffer: # if requested segment has already arrived
                res = self.receiveBuffer.pop(self.seek)
                self.seek += len(res)
                break # feed body to upper layer immediately
            else: # if not arrived yet, wait
                self.recvIntoBuffer()

        return res
        # For now, I'll just pass the full UDP payload to the app

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
