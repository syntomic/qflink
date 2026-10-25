package cn.syntomic.qflink.rule.entity;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import cn.syntomic.qflink.common.entity.Field;

public class FlatMap {

    private String pattern;

    @JsonProperty("normal_fields")
    private NormalField[] normalFields;

    @JsonProperty("mapping_fields")
    private Field[] mappingFields;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Field[] getMappingFields() {
        return mappingFields;
    }

    public void setMappingFields(Field[] mappingFields) {
        this.mappingFields = mappingFields;
    }

    public NormalField[] getNormalFields() {
        return normalFields;
    }

    public void setNormalFields(NormalField[] normalFields) {
        this.normalFields = normalFields;
    }
}
