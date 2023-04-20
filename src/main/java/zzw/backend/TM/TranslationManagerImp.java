package zzw.backend.TM;

import zzw.backend.utils.Panic;
import zzw.backend.utils.Parser;
import zzw.common.Error;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static zzw.backend.TM.Constans.TranslationManagerConstants.*;

public class TranslationManagerImp implements TranslationManager {


    //文件读写
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    //初始化
    public TranslationManagerImp(RandomAccessFile file,FileChannel fc) throws IOException {
        this.file = file;
        this.fc = fc;
        this.counterLock = new ReentrantLock();

        // 校验一个文件是否为xid格式文件
        checkXidCounter();
    }

    // 校验一个文件是否为xid格式文件
    private void checkXidCounter() {

        long length = 0;

        try {
            length = file.length();
        } catch (IOException e) {
            // xid 文件异常
            Panic.panic(Error.BadXIDFileException);
        }

        // 通过文件头的 8 字节数字反推文件的理论长度
        // 与文件的实际长度做对比。如果不同则认为 XID 文件不合法
        if(length< LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        // 开一个缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        // 从0开始读到buffer
        try {
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter =  Parser.parseLong(buffer.array());

        long end = getXidPosition(this.xidCounter+1);

        if(end!=length){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务id取得其在xid文件的对应位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH+(xid-1)*XID_FIELD_SIZE;
    }



    @Override
    public long begin() {
        this.counterLock.lock();
        
        try {
            long xid = xidCounter+1;

            updateXID(xid,FIELD_TRAN_ACTIVE);

            incrXidCounter();

            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    // 更新xid文件的状态
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0]=status;

        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void incrXidCounter() {

        xidCounter++;

        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }


    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    public boolean checkXID(long xid,byte status){

        long offset = getXidPosition(xid);

        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);


        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

    @Override
    public Boolean isActive(long xid) {
        if(xid==SUPER_XID) return  false;
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public Boolean isCommitted(long xid) {
        if(xid==SUPER_XID) return  false;
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public Boolean isAborted(long xid) {
        if(xid==SUPER_XID) return  false;
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        }catch (Exception e){
            Panic.panic(e);
        }
    }
}
