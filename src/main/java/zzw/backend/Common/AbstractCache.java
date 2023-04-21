package zzw.backend.Common;

import zzw.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    private Map<Long,T> cache; // 缓存
    private Map<Long,Integer> references;//元素的引用个数
    private Map<Long,Boolean> getting;//某个线程是否获取资源


    private int maxResource; //最大资源数
    private int count=0;//缓存中的元素个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        this.maxResource = maxResource;
        this.lock = new ReentrantLock();
    }


    // 获取资源
    protected T get(long key) throws Exception {

        while(true){

         lock.lock();


         if(getting.containsKey(key)){

             lock.unlock();

             try {
                 Thread.sleep(1);
             }catch (Exception e){
                 e.printStackTrace();
                 continue;
             }

             continue;
         }

            if(cache.containsKey(key)){

                T obj = cache.get(key);

                references.put(key,references.get(key)+1);

                lock.unlock();

                return obj;
            }

            if(maxResource>0&&count==maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }

            count++;
            getting.put(key,true);
            lock.unlock();
            break;
        }

        T obj = null;

        try {
            obj = getForCache(key);
        }catch (Exception e){
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key,obj);
        references.put(key,1);
        lock.unlock();

        return  obj;
    }

    //强制释放缓存
    protected void release(long key){
        lock.lock();

        try {
            int ref = references.get(key)-1;
            if(ref==0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(obj);
                cache.remove(key);
                count--;
            }else{
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    // 关闭缓存 写回所有资源
    protected void close(){

        lock.lock();

        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(obj);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    // 资源不在缓存的时候 获取行为
    protected abstract T getForCache(long key) throws Exception;

    // 资源被驱逐写回的行为
    protected abstract void releaseForCache(T obj);


}
