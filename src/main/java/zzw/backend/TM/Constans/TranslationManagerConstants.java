package zzw.backend.TM.Constans;

public class TranslationManagerConstants {
    // XID文件头长度
    // 记录了这个 XID 文件管理的事务的个数
    public static final int LEN_XID_HEADER_LENGTH = 8;

    // 每个事务的占用长度
    public static final int XID_FIELD_SIZE = 1;


    // 事务的三种状态
    public static final byte FIELD_TRAN_ACTIVE   = 0;
    public static final byte FIELD_TRAN_COMMITTED = 1;
    public static final byte FIELD_TRAN_ABORTED  = 2;


    // 超级事务，永远为committed状态
    public static final long SUPER_XID = 0;


    // XID 文件后缀
    public static final String XID_SUFFIX = ".xid";
}
