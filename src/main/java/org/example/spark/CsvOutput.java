package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Выгрузка в один CSV-файл. Spark пишет каталог из part-файлов, а на выходе
 * практической ждут один файл, поэтому part переносится на место результата.
 * coalesce(1) здесь допустим: все три выгрузки маленькие.
 */
public final class CsvOutput {

    private CsvOutput() {
    }

    public static void writeSingle(Dataset<Row> dataset, Path target) {
        Path stage = target.resolveSibling(target.getFileName() + ".stage");
        delete(stage);
        dataset.coalesce(1).write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("emptyValue", "")
                .csv(stage.toString());

        try (Stream<Path> files = Files.list(stage)) {
            Path part = files
                    .filter(path -> path.getFileName().toString().startsWith("part-"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Spark не создал part-файл в " + stage));
            Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        delete(stage);
    }

    private static void delete(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.delete(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
