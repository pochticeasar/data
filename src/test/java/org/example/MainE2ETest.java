package org.example;

import org.example.support.Snapshots;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Прогон через {@link Main#main}: на входе CSV-фикстура, на выходе все выгрузки,
 * сравнение со снепшотами в src/test/resources/snapshots.
 *
 * Снепшоты это зафиксированное ожидаемое поведение. Если выгрузка изменилась
 * осознанно, снепшоты обновляются командой
 * ./gradlew test -DupdateSnapshots=true
 * и diff обязательно читается глазами перед коммитом.
 */
class MainE2ETest {

    /** Все текстовые выгрузки пайплайна. Подготовленный слой это parquet, он не сравнивается. */
    private static final List<String> OUTPUTS = List.of(
            "top_products.csv",
            "customer_metrics.csv",
            "monthly_revenue.csv",
            "dq_report.csv",
            "rejected.csv",
            "rejected_ingest.csv");

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "фикстура {0}")
    @ValueSource(strings = {"greenpath", "corners", "header_only"})
    void fixtureMatchesSnapshot(String caseName) {
        Path input = Snapshots.resource("e2e/" + caseName + ".csv");
        Path outDir = tempDir.resolve(caseName);

        Main.main(new String[]{input.toString(), outDir.toString()});

        Snapshots.assertDirMatches(outDir, Snapshots.snapshotsRoot().resolve(caseName), OUTPUTS);
    }

    @Test
    void missingFileThrows() {
        Path missing = tempDir.resolve("no-such.csv");
        assertThrows(UncheckedIOException.class,
                () -> Main.main(new String[]{missing.toString(), tempDir.resolve("out").toString()}));
    }

    @Test
    void emptyFileThrows() throws Exception {
        Path empty = tempDir.resolve("empty.csv");
        Files.writeString(empty, "");
        assertThrows(IllegalArgumentException.class,
                () -> Main.main(new String[]{empty.toString(), tempDir.resolve("out").toString()}));
    }
}
