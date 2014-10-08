package cljhazelcast.remote;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.hazelcast.mapreduce.Collator;

import java.io.Serializable;

/**
 * @author codemomentum
 */
public class RemoteCollator implements Serializable, Collator {

    String fun;

    static transient IFn eval;
    static transient IFn readStr;
    static transient IFn iteratorSeq;

    static {
        eval = Clojure.var("clojure.core", "eval");
        readStr = Clojure.var("clojure.core", "read-string");
        iteratorSeq = Clojure.var("clojure.core", "iterator-seq");
    }

    public RemoteCollator(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }

    @Override
    public Object collate(Iterable iterable) {
        Object seq = iteratorSeq.invoke(iterable.iterator());
        IFn fn = (IFn) eval.invoke(readStr.invoke(fun));
        return fn.invoke(seq);
    }
}
