# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.receiveBuffer = {} # key is sequence number, value is body bytes
        self.expectingSequenceNumber = 0 # expect to receive body from
        self.sequenceNumber = 0 # sending body from

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        segmentSize = 1472
        headerSize = 64
        bodySize = segmentSize - headerSize

        for i in range(0, len(data_bytes), bodySize):
            body = data_bytes[i: i + bodySize]
            header = struct.pack(">l", self.sequenceNumber)
            segment = header + body
            self.socket.sendto(segment, (self.dst_ip, self.dst_port))
            self.sequenceNumber += len(body)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        while True:
            if self.expectingSequenceNumber in self.receiveBuffer: # if requested segment has already arrived
                res = self.receiveBuffer.pop(self.expectingSequenceNumber)
                self.expectingSequenceNumber += len(res)
                break # feed body to upper layer immediately
            else: # if not arrived yet, wait
                data, addr = self.socket.recvfrom()
                header = data[: 4]
                body = data[4: ]
                sequenceNumber = struct.unpack(">l", header)[0]
                self.receiveBuffer[sequenceNumber] = body # put in buffer

        return res
        # For now, I'll just pass the full UDP payload to the app

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
