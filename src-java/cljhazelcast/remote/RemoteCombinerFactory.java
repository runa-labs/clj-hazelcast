package cljhazelcast.remote;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.io.Serializable;

/**
 * @author codemomentum
 */
public class RemoteCombinerFactory implements Serializable, CombinerFactory {
    String fun;
    static transient IFn eval;
    static transient IFn readStr;

    static {
        eval = Clojure.var("clojure.core", "eval");
        readStr = Clojure.var("clojure.core", "read-string");
    }

    public RemoteCombinerFactory(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }

    @Override
    public Combiner newCombiner(Object key) {
        return new RemoteCombiner(key, fun);
    }

    public class RemoteCombiner extends Combiner {
        String fun;
        Object key;

        private volatile Object acc;

        public RemoteCombiner(Object key, String fun) {
            this.fun = fun;
            this.key = key;
        }

        @Override
        public void combine(Object value) {
            IFn fn = (IFn) eval.invoke(readStr.invoke(fun));
            Object result = fn.invoke(key, value, acc);
            if (null == result) {
                throw new RuntimeException("Bad Combiner, should return a collection, but found: null");
            }
            acc = result;
        }

        @Override
        public Object finalizeChunk() {
            Object chunk = acc;
            acc = null;
            return chunk;
        }
    }
}
