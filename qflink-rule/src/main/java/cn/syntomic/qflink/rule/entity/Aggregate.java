package cn.syntomic.qflink.rule.entity;

public class Aggregate {

    private String name;
    private String[] inputs;
    private Method method;
    private Eval filter;

    public Aggregate() {}

    public Aggregate(String name, String[] inputs, Method method, Eval filter) {
        this.name = name;
        this.inputs = inputs;
        this.method = method;
        this.filter = filter;
    }

    public enum Method {
        COUNT_DISTINCT,
        COUNT,
        SUM,
        MAX,
        MIN,
        AVG
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getInputs() {
        return inputs;
    }

    public void setInputs(String[] inputs) {
        this.inputs = inputs;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Eval getFilter() {
        return filter;
    }

    public void setFilter(Eval filter) {
        this.filter = filter;
    }
}
