package org.grapheco.lynx.func;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LynxProcedure {
    String name();
    boolean allowNull() default false;
    String description() default "";
}
