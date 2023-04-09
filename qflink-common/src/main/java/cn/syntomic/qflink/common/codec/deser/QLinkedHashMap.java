package cn.syntomic.qflink.common.codec.deser;

import java.util.LinkedHashMap;

/** Support nested map, in order to not affect jackson default map deserialize */
public class QLinkedHashMap extends LinkedHashMap<String, String> {}
