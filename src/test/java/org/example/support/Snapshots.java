package org.example.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Сравнение выходных CSV со снепшотами; обновление через -DupdateSnapshots=true. */
public final class Snapshots {

    private Snapshots() {
    }

    public static boolean updateMode() {
        return Boolean.getBoolean("updateSnapshots");
    }

    public static void assertDirMatches(Path actualDir, Path snapshotDir, List<String> files) {
        try {
            if (updateMode()) {
                Files.createDirectories(snapshotDir);
                for (String name : files) {
                    Path actual = actualDir.resolve(name);
                    assertTrue(Files.isRegularFile(actual), "нет фактического файла: " + actual);
                    Files.copy(actual, snapshotDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            }

            assertTrue(Files.isDirectory(snapshotDir), "нет каталога снепшотов: " + snapshotDir);
            for (String name : files) {
                Path actual = actualDir.resolve(name);
                Path expected = snapshotDir.resolve(name);
                assertTrue(Files.isRegularFile(actual), "нет фактического файла: " + actual);
                assertTrue(Files.isRegularFile(expected),
                        "нет снепшота " + expected + " (запусти с -DupdateSnapshots=true)");
                assertEquals(
                        normalize(Files.readString(expected, StandardCharsets.UTF_8)),
                        normalize(Files.readString(actual, StandardCharsets.UTF_8)),
                        () -> "расхождение со снепшотом: " + name);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void assertMatches(Path actualFile, Path snapshotFile) {
        try {
            if (updateMode()) {
                Files.createDirectories(snapshotFile.getParent());
                Files.copy(actualFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING);
                return;
            }
            assertTrue(Files.isRegularFile(actualFile), "нет фактического файла: " + actualFile);
            assertTrue(Files.isRegularFile(snapshotFile),
                    "нет снепшота " + snapshotFile + " (запусти с -DupdateSnapshots=true)");
            assertEquals(
                    normalize(Files.readString(snapshotFile, StandardCharsets.UTF_8)),
                    normalize(Files.readString(actualFile, StandardCharsets.UTF_8)),
                    () -> "расхождение со снепшотом: " + snapshotFile.getFileName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path resource(String classpathRelative) {
        var url = Objects.requireNonNull(
                Snapshots.class.getClassLoader().getResource(classpathRelative),
                "ресурс не найден: " + classpathRelative);
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Path snapshotsRoot() {
        return Path.of(System.getProperty("snapshots.dir", "src/test/resources/snapshots"));
    }

    private static String normalize(String text) {
        return text.replace("\r\n", "\n");
    }
}
