package cn.syntomic.qflink.common.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileReaderTest {

    @Test
    void testReadFromFileSystem(@TempDir Path dir) throws Exception {
        Path sql = dir.resolve("demo.sql");
        Files.writeString(sql, "SELECT 1;");

        ReaderFactory reader = ReaderFactory.of("file");
        assertTrue(reader instanceof FileReader);
        assertEquals("SELECT 1;", reader.readFile(sql.toString()));
    }

    @Test
    void testBaseDir(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir.resolve("sqls"));
        Files.writeString(dir.resolve("sqls/demo.sql"), "SELECT 2;");

        Configuration conf = new Configuration();
        conf.set(FileReader.BASE_DIR, dir.toString());
        assertEquals("SELECT 2;", ReaderFactory.of("file").readFile("sqls/demo.sql", conf));
    }

    @Test
    void testMissingFile(@TempDir Path dir) {
        assertThrows(
                IOException.class,
                () -> ReaderFactory.of("file").readFile(dir.resolve("absent.sql").toString()));
    }
}
