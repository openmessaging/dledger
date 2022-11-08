package io.openmessaging.storage.dledger;/* 
    create qiangzhiwei time 2022/11/8
 */

public class ChangePeersFuture<T> extends TimeoutFuture<T>{
    public ChangePeersFuture(long timeOutMs) {
        super(timeOutMs);
    }
}
