package com.atguigu.gmall2013.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall2013.publisher.mapper")
public class Gmall0213PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0213PublisherApplication.class, args);
    }

}
