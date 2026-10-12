package org.example.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Разбиение parquet-файла на диапазоны по границам row group. */
public final class RowGroups {

    private RowGroups() {
    }

    /** По одному диапазону на row group, в порядке файла. */
    public static List<ByteRange> ranges(Path path) {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(path),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            var blocks = reader.getFooter().getBlocks();
            List<ByteRange> ranges = new ArrayList<>(blocks.size());
            for (var block : blocks) {
                long start = block.getStartingPos();
                ranges.add(new ByteRange(start, start + block.getCompressedSize()));
            }
            return ranges;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static long rowCount(Path path) {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(path),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build())) {
            return reader.getFooter().getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
