package com.hmdp.utils;
import ch.qos.logback.core.util.ExecutorServiceUtil;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);

    }

    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //封装RedisData
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));

    }

    /**
     * //根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     * // 使用泛型、支持不同类型的参数和返回值
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time,TimeUnit unit){
        // 组装用于查询或写入redis的key
        String key = keyPrefix + id;
        //1 尝试获取redis缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2 判断缓存是否存在
        if(StrUtil.isNotBlank(json)){
            //3 存在则返回缓存信息
            return JSONUtil.toBean(json,type);
        }
        //判断是否是空值
        if(json != null){
            return null;
        }
        //4 不存在，查询数据库（引入function函数）
        R r = dbFallback.apply(id);
        //5 判断数据库是否存在
        if(r == null){
            //6 不存在（也在redis中存null 防止缓存穿透）返回null
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //7 存在、写入redis，返回商铺信息
        this.set(key,r,time,unit);
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    /**
     * //根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time,TimeUnit unit){
        // 组装用于查询或写入redis的key
        String key = keyPrefix + id;
        //1 尝试获取redis缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2 判断缓存是否存在
        if(StrUtil.isBlank(json)){ //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3 不存在则返回null
            return null;
        }
        //4 存在，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json,RedisData.class);
        Object data = redisData.getData();
        R r = JSONUtil.toBean((JSONObject) data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1 未过期直接返回信息
            return r;
        }

        //5.2 过期了，需要进行缓存重建
        //6 缓存重建
        //6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean flag = tryLock(lockKey);

        //6.2 判断互斥锁是否获取成功
        if(flag){
            //6.3 获取成功 则开启独立线程、实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    // 释放锁
                    unlock(key);
                }
            });
        }


        //6.4 获取失败，则返回过期的信息
        return r;

    }

    /**
     * 创建锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return flag;
    }

    /**
     * 封闭锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
