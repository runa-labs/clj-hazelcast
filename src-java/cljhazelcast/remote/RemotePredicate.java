package cljhazelcast.remote;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.Map;

/**
 * @author codemomentum
 */
public class RemotePredicate implements Serializable, Predicate {

    String fun;
    static transient IFn eval;
    static transient IFn readStr;

    static {
        eval = Clojure.var("clojure.core", "eval");
        readStr = Clojure.var("clojure.core", "read-string");
    }

    public RemotePredicate(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }

    @Override
    public boolean apply(Map.Entry entry) {
        IFn fn = (IFn) eval.invoke(readStr.invoke(fun));
        Object result = fn.invoke(entry.getKey(), entry.getValue());
        if (!(result instanceof Boolean)) {
            throw new RuntimeException("Bad predicate, should return a bool, but found: " + result.getClass());
        }
        return (Boolean) result;
    }
}
