package com.s3.ftp.jupiter;

import com.robothy.s3.jupiter.extensions.LocalS3EndpointResolver;
import com.robothy.s3.jupiter.extensions.S3ClientResolver;
import com.robothy.s3.jupiter.supplier.DataPathSupplier;
import com.robothy.s3.rest.bootstrap.LocalS3Mode;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(LocalS3Extension.class)
@ExtendWith(S3ClientResolver.class)
@ExtendWith(LocalS3EndpointResolver.class)
public @interface LocalS3 {

    /**
     * Set the TCP port that LocalS3 is listen to.
     *
     * @return the TCP port that LocalS3 is listen to.
     */
    int port() default -1;

    /**
     * Set LocalS3 running mode.
     *
     * @return the running mode.
     */
    LocalS3Mode mode() default LocalS3Mode.IN_MEMORY;

    /**
     * Set the data path of LocalS3 service. If LocalS3 runs in {@code PERSISTENCE} mode,
     * then all data is fetch from and stores in the specified path. If LocalS3 runs in {@code IN_MEMORY}
     * mode, then LocalS3 loads initial data from the specified path.
     *
     * @return the data path of LocalS3.
     */
    String dataPath() default "";

    /**
     * Set the data path supplier class for LocalS3 service. The class implements
     * the {@linkplain DataPathSupplier} interface and must have a no-args constructor.
     * This option is used fot the scenario that the data path is generated dynamically.
     *
     * @return a data path supplier class.
     */
    Class<? extends DataPathSupplier> dataPathSupplier() default DataPathSupplier.class;

    /**
     * Set if enable the initial data cache. This option only available when running LocalS3
     * in {@code IN_MEMORY} mode with initial data. If the cache enabled, LocalS3 will cache
     * the accessed data of the path; when start LocalS3 in other tests with the same {@code dataPath},
     * the cached data will be fetched.
     *
     * @return if initial data cache enabled.
     */
    boolean initialDataCacheEnabled() default true;
}