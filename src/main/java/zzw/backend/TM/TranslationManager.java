package zzw.backend.TM;

import zzw.backend.TM.Constans.TranslationManagerConstants;
import zzw.backend.utils.Panic;
import zzw.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static zzw.backend.TM.Constans.TranslationManagerConstants.LEN_XID_HEADER_LENGTH;
import static zzw.backend.TM.Constans.TranslationManagerConstants.XID_SUFFIX;

public interface TranslationManager {
    // 事务的开始 提交 取消方法
    long begin();
    void commit(long xid);
    void abort(long xid);

    //检查某个事物的状态 通过xid事务id
    Boolean isActive(long xid);
    Boolean isCommitted(long xid);
    Boolean isAborted(long xid);

    //关闭事务管理器
    void close();


    //另外就是两个静态方法：
    // create() 和 open()，分别表示创建一个
    // xid 文件并创建 TM 和从一个已有的 xid 文件来创建 TM
    // 从零创建 XID 文件时 需要写一个空的 XID 文件头
    // 即设置 xidCounter 为 0 否则后续在校验时会不合法

    static TranslationManagerImp creat(String path) throws IOException {
        File file = new File(path + XID_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }

        if(!file.canRead()||!file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

         RandomAccessFile raf=null;
         FileChannel fc=null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        }catch (Exception e){
            Panic.panic(e);
        }

        return new TranslationManagerImp(raf,fc);
    }

    static TranslationManagerImp open(String path) throws IOException {

        File file = new File(path + XID_SUFFIX);


        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }


         RandomAccessFile raf=null;
         FileChannel fc=null;

        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        }catch (Exception e){
            Panic.panic(e);
        }

        return new TranslationManagerImp(raf,fc);
    }
}
