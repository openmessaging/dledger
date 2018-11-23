package io.openmessaging.storage.dleger;

public class AppendFuture<T> extends TimeoutFuture<T> {


   private long pos;


    public AppendFuture() {

    }

    public AppendFuture(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }
}
