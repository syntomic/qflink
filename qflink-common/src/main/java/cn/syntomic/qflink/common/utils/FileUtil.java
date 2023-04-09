package cn.syntomic.qflink.common.utils;

import static java.io.File.separator;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

public class FileUtil {

    /**
     * Get the original file path
     *
     * @param dir file directory
     * @param filePath absolute or relative file path
     * @return
     */
    public static Path getRelPath(@Nullable String dir, String filePath) {
        // compatible with absolute path
        if (filePath.startsWith(separator)) {
            return Paths.get(filePath);
        } else {
            // target to src dir, only support standard maven structure
            String[] targetPath =
                    FileUtil.class.getClassLoader().getResource("").getPath().split(separator);
            int pathLen = targetPath.length;
            targetPath[pathLen - 2] = "src";
            targetPath[pathLen - 1] =
                    targetPath[pathLen - 1].startsWith("test")
                            ? Paths.get("test", "resources", dir).toString()
                            : Paths.get("main", "resources", dir).toString();

            return Paths.get(String.join(separator, targetPath), filePath);
        }
    }

    private FileUtil() {}
}
