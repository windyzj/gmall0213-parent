package com.atguigu.gmall2013.publisher.mapper;

import com.atguigu.gmall2013.publisher.bean.HourAmount;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {
    // 查询总额
    public BigDecimal getOrderTotalAmount(String dt);

    // 查询分时金额
    public List<HourAmount> getOrderHourAmount(String dt);

}
