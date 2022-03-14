/*
 * Copyright 2021 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package javax.annotation;

import java.lang.annotation.*;


/**
 * gRPC generated code uses the @Generated annotation which is no longer part of core JDK
 * The JavaEE version is CDDL licensed, which would introduce a CDDL dependency
 *
 * This version of the @Generated annotation is copied from the Tomcat Jakarta implementation,
 * which is an Apache-2.0 licensed implementation.
 *
 * https://github.com/apache/tomcat/blob/main/java/jakarta/annotation/Generated.java
 *
 * Discussion on gRPC issue is here:
 * https://github.com/grpc/grpc-java/issues/6833
 *
 * The @Generated annotation is also used by Google cloud platform libraries
 */
@Documented
@Target({ElementType.ANNOTATION_TYPE, ElementType.CONSTRUCTOR,
        ElementType.FIELD, ElementType.LOCAL_VARIABLE, ElementType.METHOD,
        ElementType.PACKAGE, ElementType.PARAMETER, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Generated {

    /**
     * @return The name of the code generator. It is recommended that the fully
     *         qualified name of the code generator is used.
     */
    String[] value();

    /**
     * @return The date the code was generated
     */
    String date() default "";

    /**
     * @return Additional comments (if any) related to the code generation
     */
    String comments() default "";
}
