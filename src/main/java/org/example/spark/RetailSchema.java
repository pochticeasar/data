package org.example.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;

/** Схема источника и границы периода датасета. */
public final class RetailSchema {

    /** Формат даты в CSV: 12/1/2010 8:26. */
    public static final String DATE_FORMAT = "M/d/yyyy H:mm";

    /** Файл не в UTF-8. */
    public static final String ENCODING = "ISO-8859-1";

    /** Деньги: точность с запасом, масштаб 4 покрывает 3 знака в UnitPrice. */
    public static final String MONEY_TYPE = "decimal(20,4)";

    public static final String OUTPUT_MONEY_TYPE = "decimal(20,2)";

    /** Период по описанию датасета на UCI: 01.12.2010 - 09.12.2011. */
    public static final LocalDate PERIOD_START = LocalDate.of(2010, 12, 1);
    public static final LocalDate PERIOD_END = LocalDate.of(2011, 12, 9);

    /**
     * Читаем всё строками и приводим типы сами: так битое значение видно как
     * конкретная причина отбраковки, а не как молчаливый null от Spark.
     */
    public static final StructType SOURCE = new StructType()
            .add("InvoiceNo", DataTypes.StringType)
            .add("StockCode", DataTypes.StringType)
            .add("Description", DataTypes.StringType)
            .add("Quantity", DataTypes.StringType)
            .add("InvoiceDate", DataTypes.StringType)
            .add("UnitPrice", DataTypes.StringType)
            .add("CustomerID", DataTypes.StringType)
            .add("Country", DataTypes.StringType);

    private RetailSchema() {
    }
}
