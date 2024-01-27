package cn.syntomic.qflink.sql.udf.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

/**
 * Returns strings from raw string which extracted with a specified regular expression regex with
 * all match groups.
 */
public class QRegexExtract extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;

    private Pattern p;

    public void eval(String raw, String regex, String... fields) {
        Matcher m = p.matcher(raw);

        if (m.matches()) {
            collect(Row.of(IntStream.range(1, fields.length + 1).mapToObj(m::group).toArray()));
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(
                        callContext -> {
                            int argsLen = callContext.getArgumentDataTypes().size();

                            if (argsLen <= 2) {
                                throw callContext.newValidationError(
                                        "Must have at least 3 arguments.");
                            }

                            if (!callContext.isArgumentLiteral(1)
                                    || callContext.isArgumentNull(1)) {
                                throw callContext.newValidationError(
                                        "Literal expected for second argument.");
                            }

                            p =
                                    Pattern.compile(
                                            callContext.getArgumentValue(1, String.class).get());
                            List<Field> fields = new ArrayList<>(argsLen - 2);

                            for (int i = 2; i < argsLen; i++) {
                                // use literal parameter as field name
                                String fieldName =
                                        callContext
                                                .getArgumentValue(i, String.class)
                                                .orElse("f" + (i - 2));
                                fields.add(i - 2, DataTypes.FIELD(fieldName, DataTypes.STRING()));
                            }

                            return Optional.of(DataTypes.ROW(fields.toArray(new Field[0])));
                        })
                .build();
    }

    @VisibleForTesting
    public void setPattern(String regex) {
        this.p = Pattern.compile(regex);
    }
}
