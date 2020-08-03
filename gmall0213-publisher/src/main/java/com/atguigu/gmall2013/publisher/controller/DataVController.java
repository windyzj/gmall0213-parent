package com.atguigu.gmall2013.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall2013.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    OrderService orderService;

    @GetMapping("/trademark")
     public String trademarkStat(@RequestParam("startDt")String startDt,@RequestParam("endDt") String endDt){
        List<Map> trademarkSumList = orderService.getTrademarkSum(startDt, endDt);

        List<Map> rsList=new ArrayList<>();
        for (Map trademarkMap : trademarkSumList) {
            String tm_name =(String) trademarkMap.get("tm_name");
            BigDecimal amount =(BigDecimal) trademarkMap.get("amount");
            HashMap<String, Object> rsMap = new HashMap<>();
            rsMap.put("x",tm_name);
            rsMap.put("y",amount);
            rsMap.put("s","1");
            rsList.add(rsMap);
        }

        return JSON.toJSONString(rsList);


    }


}
