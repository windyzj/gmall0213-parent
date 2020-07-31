package com.atguigu.gmall2013.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall2013.publisher.service.DauService;
import com.atguigu.gmall2013.publisher.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@Slf4j
public class PublisherController {

    @Autowired
    DauService dauService;

    @Autowired
    OrderService orderService;

    @GetMapping("realtime-total")
    public  String   realtimeTotal(@RequestParam("date") String date){

        List<Map<String,Object>> totalList=new ArrayList<>();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal=0L;
        try {
              dauTotal = dauService.getDauTotal(date);
        }catch (Exception e){
            log.error("ES 服务错误！！");
            e.printStackTrace();
        }

        dauMap.put("value",dauTotal );
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233 );

        totalList.add(newMidMap);

        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        BigDecimal orderTotalAmount = orderService.getOrderTotalAmount(date);
        orderAmountMap.put("value",orderTotalAmount );
        totalList.add(orderAmountMap);

        return   JSON.toJSONString(totalList) ;
    }

    @RequestMapping("realtime-hour")
    public String realtimeHour(@RequestParam("date") String date,@RequestParam("id") String id){

        if("dau".equals(id)){ //日活
            Map dauHourCountTodayMap = dauService.getDauHourCount(date);
            String yd = getYd(date);
            Map dauHourCountYesterdayMap = dauService.getDauHourCount(yd);

            Map<String,Map<String,Long>> hourCountMap=new HashMap<>();
            hourCountMap.put("yesterday",dauHourCountYesterdayMap);
            hourCountMap.put("today",dauHourCountTodayMap);
            return  JSON.toJSONString(hourCountMap);
        }else if("order_amount".equals(id)){
            Map orderAmountTDMap = orderService.getOrderHourAmount(date);
            String yd = getYd(date);
            Map orderAmountYDMap = orderService.getOrderHourAmount(yd);

            Map<String,Map<String,Long>> hourAmountMap=new HashMap<>();
            hourAmountMap.put("yesterday",orderAmountYDMap);
            hourAmountMap.put("today",orderAmountTDMap);
            return  JSON.toJSONString(hourAmountMap);
        }
        return null;

    }

    private String getYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date tdDate = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            return   simpleDateFormat.format(ydDate);

        } catch (ParseException e) {
            throw new RuntimeException("格式转换有误");
        }


    }
}
