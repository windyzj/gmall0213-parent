package com.atguigu.gmall0213.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.slf4j.SLF4JLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;



@RestController    //=@controller @response
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping("/applog")
     public String applog(@RequestBody String json){
        System.out.println(json);
        JSONObject jsonObject = JSON.parseObject(json);


        if(jsonObject.getString("start")!=null &&jsonObject.getString("start").length()>0 ){
            kafkaTemplate.send("GMALL_START0213",json);
        }else{
            kafkaTemplate.send("GMALL_EVENT0213",json);
        }

        log.info(json);


        return "ok";
     }

}
