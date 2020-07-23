package com.atguigu.gmall2013.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall2013.publisher.service.DauService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;

    @RequestMapping("realtime-total")
    public  String   realtimeTotal(@RequestParam("date") String date){

        List<Map<String,Object>> totalList=new ArrayList<>();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = dauService.getDauTotal(date);
        dauMap.put("value",dauTotal );
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233 );
        totalList.add(newMidMap);

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
