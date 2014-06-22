package cljhazelcast.remote;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.io.Serializable;

/**
 * @author codemomentum
 */
public class RemoteMapper implements Serializable, Mapper {

    //todo
    final static String emitterFun = "(fn [pairs collector] (doall (map #(do (let [emit-key (first %1) emit-value (second %1)] (.emit collector emit-key emit-value))) pairs)))";

    String fun;

    static transient IFn eval;
    static transient IFn readStr;
    static transient IFn emitter;

    static {
        eval = Clojure.var("clojure.core", "eval");
        readStr = Clojure.var("clojure.core", "read-string");
        emitter = (IFn) eval.invoke(readStr.invoke(emitterFun));
    }

    public RemoteMapper(String fun) {
        this.fun = fun;
    }


    public String getFun() {
        return fun;
    }

    @Override
    public void map(Object key, Object value, Context context) {
        IFn fn = (IFn) eval.invoke(readStr.invoke(fun));
        Object result = fn.invoke(key, value);
        if (null == result) {
            throw new RuntimeException("Bad mapper, should return a collection, but found: null");
        } else if (!(result instanceof IPersistentCollection)) {
            throw new RuntimeException("Bad mapper, should return a collection, but found: " + result.getClass());
        }
        emitter.invoke(result, context);
    }

}
