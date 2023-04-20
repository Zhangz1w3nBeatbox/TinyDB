package zzw.backend.TM;

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

}
