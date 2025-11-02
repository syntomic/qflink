package cn.syntomic.qflink.sql.sdk.jobs;

import static cn.syntomic.qflink.sql.sdk.configuration.SQLOptions.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.text.StrSubstitutor;

import cn.syntomic.qflink.common.readers.ReaderFactory;

public class SQLFactory {

    private static final Pattern SQL_SEPARATOR = Pattern.compile("; *$", Pattern.MULTILINE);
    private static final Pattern SQL_COMMENT = Pattern.compile("--.*$", Pattern.MULTILINE);
    private static final String SQL_DIR = "sqls";

    private final String sqlFile;
    private final ReaderFactory readerFactory;

    private final StrSubstitutor sub;

    private SQLFactory(String sqlFile, ReaderFactory readerFactory, Configuration conf) {
        this.sqlFile = sqlFile;
        this.readerFactory = readerFactory;
        this.sub = new StrSubstitutor(conf.toMap());
    }

    /**
     * Read executable sqls from specific sql files
     *
     * @return
     */
    public String[] readSqls() throws IOException {
        return splitSql(readerFactory.readFile(Paths.get(SQL_DIR, sqlFile).toString()));
    }

    /**
     * Split multi sqls to single flink executable sql
     *
     * @param sqlFile
     * @return
     */
    private String[] splitSql(String sqlFile) {
        // replace variable
        String sqls = sub.replace(sqlFile);
        // remove comments
        sqls = SQL_COMMENT.matcher(sqls).replaceAll("").trim();
        return SQL_SEPARATOR.split(sqls);
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
