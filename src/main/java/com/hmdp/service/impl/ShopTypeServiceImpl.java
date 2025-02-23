package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

//    @Override
//    public Result querySort() {
//        List<String> shopTypeJson = stringRedisTemplate.opsForList().range(RedisConstants.SHOP_TYPE_KEY, 0, -1);
//        if (shopTypeJson != null && !shopTypeJson.isEmpty()) {
//            List<ShopType> shopTypes = shopTypeJson.stream()
//                    .map(shopType -> JSONUtil.toBean(shopType, ShopType.class))
//                    .collect(Collectors.toList());
//            return Result.ok(shopTypes);
//        }
//        List<ShopType> shopTypes = query().orderByAsc("sort").list();
//        if(shopTypes==null||shopTypes.isEmpty()){
//            return Result.fail("没有分类数据");
//        }
//        for (ShopType shopType : shopTypes) {
//            String json = JSONUtil.toJsonStr(shopType);
//            stringRedisTemplate.opsForList().rightPush(RedisConstants.SHOP_TYPE_KEY,json);
//        }
//        return Result.ok(shopTypes);
//
////        // 1.先从redis缓存看是否存在
////         String shopTypeJson = stringRedisTemplate.opsForValue().get(RedisConstants.SHOP_TYPE_KEY);
////        if(StrUtil.isNotBlank(shopTypeJson)) {
////            //2.如果存在，返回分类信息
////            return Result.ok(JSONUtil.toList(shopTypeJson, ShopType.class));
////        }
////      //3.不存在，则查询数据库
////        List<ShopType> shopTypes = query().orderByAsc("sort").list();
////        if(shopTypes==null) {
////            //4.不存在，则返回错误信息
////            return Result.fail("没有分类数据");
////        }
////        // 5.数据库存在在，则存入redis，返回分类信息
////        String json = StrUtil.toString(shopTypes);
////        stringRedisTemplate.opsForValue().set(RedisConstants.SHOP_TYPE_KEY,json);
////
////        return Result.ok(shopTypes);
//    }

    @Override
    public Result querySort() {
        // 1. 从 Redis 获取数据
        String shopTypeJson = stringRedisTemplate.opsForValue().get(RedisConstants.SHOP_TYPE_KEY);

        // 2. Redis 有数据，解析并返回
        if (StrUtil.isNotBlank(shopTypeJson)) {
            try {
                List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
                return Result.ok(shopTypes);
            } catch (Exception e) {
                // JSON 解析错误时，打印日志并继续查询数据库
                e.printStackTrace();
                System.out.println("Redis JSON 格式错误：" + shopTypeJson);
            }
        }

        // 3. Redis 没有数据，从数据库查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if (shopTypes == null || shopTypes.isEmpty()) {
            return Result.fail("没有分类数据");
        }

        // 4. 存入 Redis，确保存储格式正确
        stringRedisTemplate.opsForValue().set(RedisConstants.SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypes));

        // 5. 返回数据
        return Result.ok(shopTypes);
    }
}
