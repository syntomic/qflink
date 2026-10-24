package cn.syntomic.qflink.sql.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import cn.syntomic.qflink.sql.api.SqlTemplate.BoundSql;

/**
 * The unified request of a table: filters, order and paging as data instead of a sql per api.
 *
 * <pre>{@code
 * {"filters": {"condition_operator": "AND",
 *              "filters": [{"field": "area", "filter_operator": "EQUAL", "value": "美国"}]},
 *  "order_by": [{"order_field": "play_cnt", "order_type": "DESC"}],
 *  "limit": 20, "offset": 0}
 * }</pre>
 *
 * <p>Every field, operator and order type is checked against a whitelist before it reaches the sql,
 * the values are bound.
 */
public class QueryProtocol {

    /** Filter operators and the sql they stand for, {@code ?} is the bound value. */
    private static final Map<String, String> OPERATORS =
            Map.of(
                    "EQUAL", "= ?",
                    "NOT_EQUAL", "<> ?",
                    "GREATER", "> ?",
                    "GREATER_EQUAL", ">= ?",
                    "LESS", "< ?",
                    "LESS_EQUAL", "<= ?",
                    "LIKE", "LIKE ?");

    private final String table;
    private final Set<String> fields;
    private final long maxLimit;

    public QueryProtocol(String table, Set<String> fields, long maxLimit) {
        this.table = table;
        this.fields = fields;
        this.maxLimit = maxLimit;
    }

    /**
     * Build the page query and the count query of the request
     *
     * @return the page query first, then the total count with the same filters
     * @throws IllegalArgumentException when the request uses anything not on the whitelists
     */
    public List<BoundSql> build(Request request) {
        List<Object> args = new ArrayList<>();
        String where = where(request.filters, args);

        StringBuilder order = new StringBuilder();
        for (Order item : request.orderBy) {
            field(item.field);
            String type = item.type == null ? "ASC" : item.type.toUpperCase();
            if (!type.equals("ASC") && !type.equals("DESC")) {
                throw new IllegalArgumentException("order_type must be ASC or DESC, got " + type);
            }
            order.append(order.length() == 0 ? " ORDER BY " : ", ")
                    .append(item.field)
                    .append(' ')
                    .append(type);
        }

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(SqlTemplate.number("limit", String.valueOf(request.limit), maxLimit));
        pageArgs.add(SqlTemplate.number("offset", String.valueOf(request.offset), Long.MAX_VALUE));
        String select = String.join(", ", fields);
        return List.of(
                new BoundSql(
                        "SELECT " + select + " FROM " + table + where + order + " LIMIT ? OFFSET ?",
                        pageArgs),
                new BoundSql("SELECT COUNT(*) AS total FROM " + table + where, args));
    }

    private String where(Filters filters, List<Object> args) {
        if (filters == null || filters.filters.isEmpty()) {
            return "";
        }
        String condition = filters.condition == null ? "AND" : filters.condition.toUpperCase();
        if (!condition.equals("AND") && !condition.equals("OR")) {
            throw new IllegalArgumentException(
                    "condition_operator must be AND or OR, got " + condition);
        }

        List<String> parts = new ArrayList<>();
        for (Filter filter : filters.filters) {
            field(filter.field);
            String operator = OPERATORS.get(String.valueOf(filter.operator).toUpperCase());
            if (operator == null) {
                throw new IllegalArgumentException(
                        "filter_operator must be one of "
                                + OPERATORS.keySet()
                                + ", got "
                                + filter.operator);
            }
            parts.add(filter.field + " " + operator);
            args.add(filter.value);
        }
        return " WHERE " + String.join(" " + condition + " ", parts);
    }

    private void field(String field) {
        if (!fields.contains(field)) {
            throw new IllegalArgumentException("field must be one of " + fields + ", got " + field);
        }
    }

    /** The request body. */
    public static class Request {
        public Filters filters;

        @JsonProperty("order_by")
        public List<Order> orderBy = List.of();

        public long limit = 20;
        public long offset = 0;
    }

    /** Filters joined by one condition operator. */
    public static class Filters {
        @JsonProperty("condition_operator")
        public String condition;

        public List<Filter> filters = List.of();
    }

    /** One filter. */
    public static class Filter {
        public String field;

        @JsonProperty("filter_operator")
        public String operator;

        public Object value;
    }

    /** One order item. */
    public static class Order {
        @JsonProperty("order_field")
        public String field;

        @JsonProperty("order_type")
        public String type;
    }
}
