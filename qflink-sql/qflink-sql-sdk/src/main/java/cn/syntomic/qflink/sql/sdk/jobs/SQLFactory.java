package cn.syntomic.qflink.sql.sdk.jobs;

import static cn.syntomic.qflink.sql.sdk.configuration.SQLOptions.EXECUTE_SQL;
import static cn.syntomic.qflink.sql.sdk.configuration.SQLOptions.READ_TYPE;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;

import cn.syntomic.qflink.common.readers.ReaderFactory;

public class SQLFactory {

    private static final String SQL_DIR = "sqls";

    private final String sqlFile;
    private final ReaderFactory readerFactory;

    private final Configuration conf;
    private final StrSubstitutor sub;

    private SQLFactory(String sqlFile, ReaderFactory readerFactory, Configuration conf) {
        this.sqlFile = sqlFile;
        this.readerFactory = readerFactory;
        this.conf = conf;
        this.sub = new StrSubstitutor(conf.toMap());
    }

    /**
     * Read executable sqls from specific sql files
     *
     * @return
     */
    public String[] readSqls() throws IOException {
        return splitSql(readerFactory.readFile(Paths.get(SQL_DIR, sqlFile).toString(), conf));
    }

    /**
     * Split multi sqls to single flink executable sql
     *
     * @param sqlFile
     * @return
     */
    private String[] splitSql(String sqlFile) {
        // replace variable
        return splitStatements(sub.replace(sqlFile));
    }

    /**
     * Split the statements on ';' and strip the comments
     *
     * <p><b>';' and '--' inside string literals or quoted identifiers are kept as they are.</b>
     *
     * @param sqls
     * @return
     */
    static String[] splitStatements(String sqls) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sqls.length(); i++) {
            char c = sqls.charAt(i);
            char next = i + 1 < sqls.length() ? sqls.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
                continue;
            }

            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
                if (c == '-' && next == '-') {
                    inLineComment = true;
                    i++;
                    continue;
                }
                if (c == '/' && next == '*') {
                    inBlockComment = true;
                    i++;
                    continue;
                }
                if (c == ';') {
                    addStatement(statements, current);
                    current.setLength(0);
                    continue;
                }
            }

            if (c == '\'' && !inDoubleQuote && !inBacktick) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote && !inBacktick) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            }

            current.append(c);
        }

        addStatement(statements, current);
        return statements.toArray(new String[0]);
    }

    private static void addStatement(List<String> statements, StringBuilder statement) {
        String sql = statement.toString().trim();
        if (StringUtils.isNotBlank(sql)) {
            statements.add(sql);
        }
    }

    /**
     * Get SQLFactory by job configuration
     *
     * @param conf
     * @return
     */
    public static SQLFactory of(Configuration conf) {
        String sqlFile = conf.get(EXECUTE_SQL);
        Preconditions.checkNotNull(sqlFile, "Param `sql.execute-sql` must not be null");

        String sqlReadType = conf.get(READ_TYPE);
        return new SQLFactory(sqlFile, ReaderFactory.of(sqlReadType), conf);
    }
}
