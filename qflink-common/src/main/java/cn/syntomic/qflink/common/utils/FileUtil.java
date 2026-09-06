package cn.syntomic.qflink.common.utils;

import static java.io.File.separator;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

public class FileUtil {

    /**
     * Get the original file path
     *
     * <p><b>Only works when the classes are loaded from a standard maven target directory, in other
     * words in the IDE or in a local run, not from a shaded jar.</b>
     *
     * @param dir file directory
     * @param filePath absolute or relative file path
     * @return
     */
    public static Path getRelPath(@Nullable String dir, String filePath) {
        // compatible with absolute path
        if (filePath.startsWith(separator)) {
            return Paths.get(filePath);
        }

        // target to src dir, only support standard maven structure
        String[] targetPath = classesDir().split(separator);
        int pathLen = targetPath.length;
        if (pathLen < 2 || !"target".equals(targetPath[pathLen - 2])) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot resolve %s: the qfile source reads from the maven source"
                                    + " directory and can only be used in a local run, but the"
                                    + " classes are loaded from %s",
                            filePath, String.join(separator, targetPath)));
        }

        targetPath[pathLen - 2] = "src";
        targetPath[pathLen - 1] =
                targetPath[pathLen - 1].startsWith("test")
                        ? Paths.get("test", "resources", dir).toString()
                        : Paths.get("main", "resources", dir).toString();

        return Paths.get(String.join(separator, targetPath), filePath);
    }

    /** The directory the classes are loaded from, decoded to a usable file system path. */
    private static String classesDir() {
        URL classes = FileUtil.class.getClassLoader().getResource("");
        if (classes == null) {
            throw new IllegalStateException(
                    "Cannot locate the classes directory, the qfile source can only be used in a"
                            + " local run");
        }

        try {
            // ! the url is encoded, a path with spaces would be broken otherwise
            return URLDecoder.decode(classes.getPath(), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Cannot decode path " + classes.getPath(), e);
        }
    }

    private FileUtil() {}
}
