package cljhazelcast.remote;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.io.Serializable;

/**
 * @author codemomentum
 */
public class RemoteReducerFactory implements Serializable, ReducerFactory {

    String fun;
    static transient IFn eval;
    static transient IFn readStr;

    static {
        eval = Clojure.var("clojure.core", "eval");
        readStr = Clojure.var("clojure.core", "read-string");
    }

    public RemoteReducerFactory(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }

    @Override
    public Reducer newReducer(Object key) {
        return new RemoteReducer(key, fun);
    }

    public class RemoteReducer extends Reducer {
        String fun;
        Object key;

        private volatile Object acc;

        public RemoteReducer(Object key, String fun) {
            this.fun = fun;
            this.key = key;
        }

        @Override
        public void reduce(Object value) {
            IFn fn = (IFn) eval.invoke(readStr.invoke(fun));
            Object result = fn.invoke(key, value, acc);
            if (null == result) {
                throw new RuntimeException("Bad Reducer, should return a collection, but found: null");
            }
            acc = result;
        }

        @Override
        public Object finalizeReduce() {
            return acc;
        }
    }

}
