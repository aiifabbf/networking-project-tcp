# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import threading
import concurrent.futures
import time
import hashlib

class Streamer:

    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.segmentSize = 1472 # maximum size of a udp body
        self.headerSize = 16 # seq 4 bytes, ack 4 bytes, checksum 8 bytes
        self.watchDogTimeout = 10 # if not hearing from anything from remote after 10 seconds when the connection is not closed, watch dog will shut down the connection
        self.closeWaitTimeout = 3 # if no progress is made in recent 3 seconds, after user has called .close(), assume remote has exited

        self.seek = 0 # upper-level application has read until

        self.pushBuffer = {} # out-bound buffer. key is sequence number, value is body bytes
        self.pushLocalSeek = 0 # I have written until
        self.pushRemoteSeek = 0 # remote has read until

        self.pullBuffer = {} # in-bound buffer. key is sequence number, value is body bytes
        self.pullLocalSeek = 0 # I have read until
        self.pullRemoteSeek = 0 # remote has written until

        self.maxInFlightSegmentCount = 8 # out-bound worker sends data packets as many as
        self.lastEchoTimeStamp = float("inf") # last time stamp when hearing from remote side, even if corrupted. It proves that remote side is alive

        self.lock = threading.Lock() # lock on push buffer, push local seek, push remote seek, pull buffer, pull local seek, pull remote seek

        self.closed = False # if closed, workers stop while-loop

        self.executor = concurrent.futures.ThreadPoolExecutor()

        self.outBoundJob = self.executor.submit(self.outBoundWorker) # out-bound worker is only in charge of sending packets
        self.inBoundJob = self.executor.submit(self.inBoundWorker) # in-bound worker is only in charge of receiving packets and updating seeks
        self.watchDogJob = self.executor.submit(self.watchDogWorker) # watch dog worker closes the connection after not hearing from remote side for too long

    def send(self, data_bytes: bytes) -> None: # application wants to send something
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        bodySize = self.segmentSize - self.headerSize

        for i in range(0, len(data_bytes), bodySize): # cut data into chunks of maximum body size
            body = data_bytes[i: i + bodySize]

            with self.lock:
                self.pushBuffer[self.pushLocalSeek] = body # do nothing but put the data in the send buffer
                self.pushLocalSeek += len(body) # update push buffer's local seek pointer further

    def outBoundWorker(self) -> None: # out-bound worker is in charge of sending data in push buffer, re-transmitting and sending heart beat packet when there are no data in push buffer

        while not self.closed:
            time.sleep(0.25) # every some time you should send something. If there is data, send data. If there is no data, send a heart beat

            with self.lock:
                if self.pushRemoteSeek < self.pushLocalSeek: # receiver did not acknowledged every segments we sent

                    for k in sorted(self.pushBuffer.keys()): # clear segments acknowledged by remote
                        v = self.pushBuffer[k]
                        if k + len(v) <= self.pushRemoteSeek:
                            self.pushBuffer.pop(k)

                    # aggregate small packets into large packets
                    data = bytearray()

                    for k in sorted(self.pushBuffer.keys()):
                        v = self.pushBuffer[k]
                        data.extend(v)

                    self.pushBuffer.clear()

                    for i in range(0, len(data), self.segmentSize - self.headerSize):
                        chunk = data[i: i + self.segmentSize - self.headerSize]
                        self.pushBuffer[i + self.pushRemoteSeek] = bytes(chunk)

                    for k in sorted(self.pushBuffer.keys())[: self.maxInFlightSegmentCount]:
                        v = self.pushBuffer[k]
                        self.sendSegment(k, self.pullLocalSeek, v)

                else: # no data in push buffer
                    self.sendAck(self.pushLocalSeek, self.pullLocalSeek) # send a heart beat then to let remote know we are alive

                # print("pull local seek:", self.pullLocalSeek)
                # print("pull remote seek:", self.pullRemoteSeek)
                # print("push local seek:", self.pushLocalSeek)
                # print("push remote seek:", self.pushRemoteSeek)

    def inBoundWorker(self) -> None: # background in-bound worker

        while not self.closed:
            if self.recvIntoBuffer():
                continue
            else:
                break

    def watchDogWorker(self) -> None: # watch dog worker shuts down the connection when it assumes that 

        while not self.closed:
            time.sleep(1)

            with self.lock:
                makingProgress = (time.time() - self.lastEchoTimeStamp) < self.watchDogTimeout

            if not makingProgress:
                # print("watchdog barks")
                # print("pull local seek:", self.pullLocalSeek)
                # print("pull remote seek:", self.pullRemoteSeek)
                # print("push local seek:", self.pushLocalSeek)
                # print("push remote seek:", self.pushRemoteSeek)
                self.close()

    def recvIntoBuffer(self) -> bool: # receive packet, decode packet, update pull buffer, pull remote seek, pull local seek, push remote seek
        data, addr = self.socket.recvfrom() # decode segment

        if not data:
            return False

        self.lock.acquire()
        self.lastEchoTimeStamp = time.time()

        try:
            decoded = self.decodeSegment(data) # if corrupted, will catch an exception here
        except ValueError:
            # print("corrupted.")
            # self.sendAck(self.pushLocalSeek, self.pullLocalSeek)
            self.lock.release()
            return True

        seq = decoded["seq"]
        ack = decoded["ack"]
        control = decoded["control"]
        body = decoded["body"]
        # print(">", seq, ack, control, body)

        self.pullRemoteSeek = max(self.pullRemoteSeek, seq + len(body)) # remote has written until
        self.pushRemoteSeek = max(self.pushRemoteSeek, ack) # remote has read until

        if body:
            self.pullBuffer[seq] = body

            if self.pullLocalSeek == self.pullRemoteSeek:
                pass
            else:
                seek = self.pullLocalSeek

                while True:
                    if seek not in self.pullBuffer:
                        break
                    else:
                        seek += len(self.pullBuffer[seek])

                self.pullLocalSeek = seek # we can acknowledge data until
        else: # no data
            pass

        self.lock.release()

        return True

    def sendAck(self, seq: int, ack: int) -> None:
        self.sendSegment(seq, ack)

    def sendSegment(self, seq: int, ack: int, body: bytes=b"") -> None:
        header = struct.pack(">ll", seq, ack)
        segment = header + body

        hasher = hashlib.sha1()
        hasher.update(segment)
        checksum = hasher.digest()[: 8] # checksum is calculated over seq, ack, body

        segment = header + checksum + body
        # print("<", seq, ack, checksum, body)
        self.socket.sendto(segment, (self.dst_ip, self.dst_port))

    def decodeSegment(self, data: bytes=b"") -> dict:
        seqack = data[: 8]
        control = data[8: 16] # checksum
        body = data[16: ]

        hasher = hashlib.sha1()
        hasher.update(seqack)
        hasher.update(body)
        checksum = hasher.digest()[: 8]

        if checksum != control:
            raise ValueError("Corrupted")

        seq, ack = struct.unpack(">ll", seqack)

        return {
            "seq": seq,
            "ack": ack,
            "control": control,
            "body": body,
        }

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        
        # this sample code just calls the recvfrom method on the LossySocket
        while True:
            if self.seek in self.pullBuffer: # if requested segment has already arrived

                with self.lock:
                    res = self.pullBuffer.pop(self.seek)
                    self.seek += len(res)

                break # feed body to upper layer immediately
            elif not self.closed: # if not arrived yet, wait
                self.recvIntoBuffer() # busy wait. could be optimized using thread conditional value
            else: # if closed
                raise Exception("Connection is already closed.")

        return res
        # For now, I'll just pass the full UDP payload to the app

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        while True:

            with self.lock:
                unsynced = self.pushLocalSeek != self.pushRemoteSeek or self.pullLocalSeek != self.pullRemoteSeek # remote side is not fully synced with local side
                makingProgress = (time.time() - self.lastEchoTimeStamp) < self.closeWaitTimeout # local side is anxious waiting to close, so timeout is smaller here
                # print("pull local seek:", self.pullLocalSeek)
                # print("pull remote seek:", self.pullRemoteSeek)
                # print("push local seek:", self.pushLocalSeek)
                # print("push remote seek:", self.pushRemoteSeek)

            if unsynced and makingProgress: # if remote and local are not fully synced but are at least making some progress (because remote is alive)
                time.sleep(0.1) # keep waiting
                # print("wait")
            else: # if fully synced or not remote is possibly dead
                # print("unsynced:", unsynced, "makingProgress:", makingProgress)
                break # close immediately

        self.socket.stoprecv()
        self.closed = True

        # print("pull buffer size:", len(self.pullBuffer))
        # print("push buffer size:", len(self.pushBuffer))
        # print("---")
        # print("pull local seek:", self.pullLocalSeek)
        # print("pull remote seek:", self.pullRemoteSeek)
        # print("push local seek:", self.pushLocalSeek)
        # print("push remote seek:", self.pushRemoteSeek)