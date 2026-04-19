package io.cursus.spring.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CursusListener {
  String topic();

  String groupId();

  String mode() default "streaming";
}
