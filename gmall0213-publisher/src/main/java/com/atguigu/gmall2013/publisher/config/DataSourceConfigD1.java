package com.atguigu.gmall2013.publisher.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;


//表示这个类为一个配置类
@Configuration
// 配置mybatis的接口类放的地方
@MapperScan(basePackages = "com.atguigu.gmall2013.publisher.mapper.d1", sqlSessionFactoryRef = "sqlSessionFactoryD1")
public class DataSourceConfigD1 {
    // 将这个对象放入Spring容器中
    @Bean(name = "dataSourceD1")
    @ConfigurationProperties(prefix = "spring.datasource.d1")
    public DataSource getDateSourceD1() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix = "mybatis.configuration")
    public org.apache.ibatis.session.Configuration globalConfigation(){
        return new org.apache.ibatis.session.Configuration();
    }


    @Bean(name = "sqlSessionFactoryD1")
    public SqlSessionFactory sqlSessionFactoryD1(@Qualifier("dataSourceD1") DataSource datasource, org.apache.ibatis.session.Configuration globalConfigation)
            throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(datasource);
        bean.setMapperLocations(
                new PathMatchingResourcePatternResolver().getResources("classpath:mapper/d1/*.xml"));

        bean.setConfiguration(globalConfigation);
        return bean.getObject();

    }
    @Bean("sqlSessionTemplateD1")
    // 表示这个数据源是默认数据源
    public SqlSessionTemplate sqlSessionTemplateD1(
            @Qualifier("sqlSessionFactoryD1") SqlSessionFactory sessionfactory) {
        return new SqlSessionTemplate(sessionfactory);
    }
}