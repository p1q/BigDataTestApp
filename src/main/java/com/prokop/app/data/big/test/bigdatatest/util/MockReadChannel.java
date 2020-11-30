package com.prokop.app.data.big.test.bigdatatest.util;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;

import java.nio.ByteBuffer;

public class MockReadChannel implements ReadChannel {
    private final byte[] toRead;
    private int position = 0;
    private boolean finished;
    private boolean isOpen;

    public MockReadChannel(String textToRead) {
        this.toRead = textToRead.getBytes();
        this.isOpen = true;
        this.finished = false;
    }

    @Override
    public void close() {
        this.isOpen = false;
    }

    @Override
    public void seek(long l) {
    }

    @Override
    public void setChunkSize(int i) {
    }

    @Override
    public RestorableState<ReadChannel> capture() {
        return null;
    }

    @Override
    public int read(ByteBuffer byteBuffer) {
        if (this.finished) {
            return -1;
        } else {
            if (byteBuffer.remaining() > this.toRead.length) {
                this.finished = true;
            }
            int toWrite = Math.min(this.toRead.length - position, byteBuffer.remaining());

            byteBuffer.put(this.toRead, this.position, toWrite);
            this.position += toWrite;

            return toWrite;
        }
    }

    @Override
    public boolean isOpen() {
        return this.isOpen;
    }
}
