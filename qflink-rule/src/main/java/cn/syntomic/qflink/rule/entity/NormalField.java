package cn.syntomic.qflink.rule.entity;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import cn.syntomic.qflink.common.entity.Field;

public class NormalField {

    private String name;
    private NormalFieldType type;

    @JsonProperty("json_paths")
    private Field[] jsonPaths;

    @JsonProperty("default")
    private Object defaulz;

    public enum NormalFieldType {
        STRING,
        JSON
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public NormalFieldType getType() {
        return type;
    }

    public void setType(NormalFieldType type) {
        this.type = type;
    }

    public Field[] getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPaths(Field[] jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public Object getDefaulz() {
        return defaulz;
    }

    public void setDefaulz(Object defaulz) {
        this.defaulz = defaulz;
    }
}
