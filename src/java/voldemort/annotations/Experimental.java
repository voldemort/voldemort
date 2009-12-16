package voldemort.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation for marking a feature as
 * "not necessarily ready for prime time". Caveat emptor!
 */
@Documented
@Inherited
@Retention(RetentionPolicy.SOURCE)
public @interface Experimental {

}
