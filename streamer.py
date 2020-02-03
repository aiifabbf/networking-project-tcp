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

        self.sendBuffer = {} # key is sequence number, value is body bytes
        self.seq = 0 # sending body from

        self.receiveBuffer = {} # key is sequence number, value is body bytes
        self.ack = 0 # expecting body from

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.executor.submit(self.inBoundWorker) # start background worker
        # self.executor.submit(self.outBoundWorker)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        segmentSize = 1472
        headerSize = 16
        bodySize = segmentSize - headerSize

        for i in range(0, len(data_bytes), bodySize):
            body = data_bytes[i: i + bodySize]
            self.sendSegment(self.seq, self.ack, body)
            self.sendBuffer[self.seq] = body
            self.seq += len(body)

        job = self.executor.submit(self.waitForAck)
        job.result()

    def waitForAck(self) -> None:

        while True:
            if self.sendBuffer: # receiver did not acked every segments we sent

                for k in sorted(self.sendBuffer.keys()): # resend everything in the send buffer
                    v = self.sendBuffer[k]
                    self.sendSegment(k, self.ack, v)

            else:
                break

    def inBoundWorker(self) -> None: # background worker

        while True:
            self.recvIntoBuffer()

    def outBoundWorker(self) -> None:

        while True:
            time.sleep(0.25)
            # print("< heartbeat", self.seq, self.ack)
            self.sendAck(self.seq, self.ack)

    def recvIntoBuffer(self) -> None: # just receive data, update self.ack and send buffer
        data, addr = self.socket.recvfrom() # decode segment
        decoded = self.decodeSegment(data)
        seq = decoded["seq"]
        ack = decoded["ack"]
        control = decoded["control"]
        body = decoded["body"]
        # print(">", seq, ack, body)

        # receiver side
        if body: # contains data
            if seq == self.ack: # normal condition, in order
                self.receiveBuffer[seq] = body
                self.ack = seq + len(body)
            elif seq > self.ack: # out of order
                self.receiveBuffer[seq] = body

                # ack frontmost continuous buffer
                seek = self.ack

                while seek != seq:
                    if seek in self.receiveBuffer:
                        seek += len(self.receiveBuffer[seek])
                    else:
                        break

                self.ack = seek
            else: # seq < self.ack, sender sent something that we already have
                pass
        else: # no data, just ack
            pass
            # print("> heartbeat", seq, ack)
        
        # sender side
        if ack == self.seq: # receiver acked all segments we sent
            self.sendBuffer.clear() # clear send buffer
        elif ack < self.seq: # receiver acked some segments we sent

            toPop = []

            for k, v in self.sendBuffer.items():
                if k + len(v) <= ack:
                    toPop.append(k)

            for k in toPop:
                self.sendBuffer.pop(k) # clear segments already acked

        else: # ack > self.seq, receiver acked some segments we have not sent
            self.sendBuffer.clear()

    def sendAck(self, seq: int, ack: int) -> None:
        self.sendSegment(seq, ack)

    def sendSegment(self, seq: int, ack: int, body: bytes=b"", control: bytes=b"") -> None:
        # print("<", seq, ack, body)
        control = control.rjust(8, b" ") # pad to 8 bytes
        header = struct.pack(">ll", seq, ack) + control
        segment = header + body
        self.socket.sendto(segment, (self.dst_ip, self.dst_port))

    def decodeSegment(self, data: bytes=b"") -> dict:
        seqack = data[: 8]
        control = data[8: 16]
        body = data[16: ]
        seq, ack = struct.unpack(">ll", seqack)
        return {
            "seq": seq,
            "ack": ack,
            "control": control.strip(),
            "body": body,
        }

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        while True:
            if self.seek in self.receiveBuffer: # if requested segment has already arrived
                res = self.receiveBuffer.pop(self.seek)
                self.seek += len(res)
                self.sendAck(self.seq, self.ack)
                break # feed body to upper layer immediately
            else: # if not arrived yet, wait
                self.recvIntoBuffer()
                self.sendAck(self.seq, self.ack)

        return res
        # For now, I'll just pass the full UDP payload to the app

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        while self.sendBuffer: # just wait until send buffer gets empty
            # print(self.sendBuffer)
            pass

        # while not self.remoteFin:
        #     pass
        self.socket.stoprecv()
        self.executor.shutdown(False)
        pass
