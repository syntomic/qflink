package cn.syntomic.qflink.common.entity;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import cn.syntomic.qflink.common.codec.deser.QArrayList;
import cn.syntomic.qflink.common.codec.deser.QLinkedHashMap;

public class Field {

    private String name;
    private FieldType type;
    private String expr;

    @JsonProperty("default")
    private Object defaulz;

    public Field() {}

    /**
     * @param name
     * @param type
     * @param expr
     * @param defaulz
     */
    public Field(String name, FieldType type, String expr, Object defaulz) {
        this.name = name;
        this.type = type;
        this.expr = expr;
        this.defaulz = defaulz;
    }

    public enum FieldType {
        STRING(String.class),
        BOOLEAN(Boolean.class),
        INT(Integer.class),
        BIGINT(Long.class),
        FLOAT(Float.class),
        DOUBLE(Double.class),

        MAP(QLinkedHashMap.class),
        LIST(QArrayList.class);

        private final Class<?> clazz;

        private FieldType(Class<?> clazz) {
            this.clazz = clazz;
        }

        public Class<?> getClazz() {
            return clazz;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FieldType getType() {
        return type;
    }

    public void setType(FieldType type) {
        this.type = type;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public Object getDefaulz() {
        return defaulz;
    }

    public void setDefaulz(Object defaulz) {
        this.defaulz = defaulz;
    }
}
