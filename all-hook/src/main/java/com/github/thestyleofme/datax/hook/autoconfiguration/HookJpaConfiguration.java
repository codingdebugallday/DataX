package com.github.thestyleofme.datax.hook.autoconfiguration;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import com.alibaba.datax.app.context.HookDatasourceContext;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/24 17:27
 * @since 1.0.0
 */
@Configuration
@ComponentScan(basePackages = {
        "com.github.thestyleofme.datax.hook"
})
@EnableJpaRepositories(basePackages = {
        "com.github.thestyleofme.datax.hook"
})
@EnableTransactionManagement
public class HookJpaConfiguration {

    @Bean
    public DataSource dataSource() {
        return HookDatasourceContext.getDatasource();
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(false);
        vendorAdapter.setDatabase(Database.MYSQL);
        vendorAdapter.setDatabasePlatform("org.hibernate.dialect.MySQLDialect");
        vendorAdapter.setShowSql(true);

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setJpaPropertyMap(jpaProperties());
        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan("com.github.thestyleofme.datax.hook");
        factory.setDataSource(dataSource);
        factory.setPersistenceProvider(new HibernatePersistenceProvider());
        return factory;
    }

    private Map<String, Object> jpaProperties() {
        Map<String, Object> props = new HashMap<>(4);
        props.put("hibernate.physical_naming_strategy", HookPhysicalNamingStrategy.class.getName());
        return props;
    }

    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory);
        return txManager;
    }
}
