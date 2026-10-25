package cn.syntomic.qflink.rule.entity;

public class Eval {
    private String expr;
    private String[] params;

    public Eval() {}

    /**
     * @param expr
     * @param params
     */
    public Eval(String expr, String[] params) {
        this.expr = expr;
        this.params = params;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public String[] getParams() {
        return params;
    }

    public void setParams(String[] params) {
        this.params = params;
    }
}
