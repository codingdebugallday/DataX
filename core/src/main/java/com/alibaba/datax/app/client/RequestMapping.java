package com.alibaba.datax.app.client;

import java.lang.annotation.*;

/**
 * <p>
 * 自定义注解实现datax服务器url的handler处理
 * 类似spring mvc
 * </p>
 *
 * @author isaac 2020/12/11 11:01
 * @since 1.0.0
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequestMapping {

    String value() default "";

    String method() default "GET";
}
