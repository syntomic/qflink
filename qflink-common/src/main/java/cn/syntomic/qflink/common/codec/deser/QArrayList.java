package cn.syntomic.qflink.common.codec.deser;

import java.util.ArrayList;

/** Support nested array, in order to not affect jackson default list deserialize */
public class QArrayList extends ArrayList<String> {}
