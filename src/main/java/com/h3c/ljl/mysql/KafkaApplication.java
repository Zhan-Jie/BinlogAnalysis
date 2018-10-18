package com.h3c.ljl.mysql;

import com.h3c.ljl.mysql.canal.CanalClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
public class KafkaApplication {

    public static void main(String[] args){
        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
        context.getBean(CanalClient.class).analysisBinlog();
    }
}
