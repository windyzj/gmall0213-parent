package com.atguigu.gmall2013.publisher.mapper.d2;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface TrademarkStatMapper {

    public List<Map> getTrademarkSum(@Param("startDt") String startDt ,@Param("endDt") String endDt);



}
