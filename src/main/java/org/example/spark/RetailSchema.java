package org.example.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.util.Set;

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
     * Форматы идентификаторов по описанию датасета на UCI:
     * InvoiceNo - 6 цифр, у отмены префикс C;
     * StockCode - 5 цифр;
     * CustomerID - 5 цифр.
     * Данные от этого отклоняются, поэтому проверки живут в измерении SCHEMA
     * как предупреждения: отбраковывать по ним нельзя, иначе датасета не останется.
     */
    public static final String INVOICE_PATTERN = "^[Cc]?[0-9]{6}$";

    /** Код товара строго по схеме. */
    public static final String STOCK_PATTERN = "^[0-9]{5}$";

    /** Код товара с буквенным суффиксом варианта: 85123A, 85099B. */
    public static final String STOCK_VARIANT_PATTERN = "^[0-9]{5}[A-Za-z]{1,2}$";

    public static final String CUSTOMER_PATTERN = "^[0-9]{5}$";

    /** Минимальное требование к идентификатору клиента: это число. */
    public static final String CUSTOMER_NUMERIC_PATTERN = "^[0-9]+$";

    /**
     * Код товара: цифры с необязательным буквенным суффиксом варианта.
     * Схема обещает ровно 5 цифр, но в данных есть и 85123A, и четырёхзначные коды,
     * поэтому для классификации используется это более слабое условие.
     */
    public static final String PRODUCT_CODE_PATTERN = "^[0-9]+[A-Za-z]{0,2}$";

    /**
     * Справочник стран собран профилированием источника: 38 значений.
     * Всё, чего здесь нет, это либо новая страна, либо опечатка.
     */
    public static final Set<String> COUNTRIES = Set.of(
            "Australia", "Austria", "Bahrain", "Belgium", "Brazil", "Canada", "Channel Islands",
            "Cyprus", "Czech Republic", "Denmark", "EIRE", "European Community", "Finland",
            "France", "Germany", "Greece", "Hong Kong", "Iceland", "Israel", "Italy", "Japan",
            "Lebanon", "Lithuania", "Malta", "Netherlands", "Norway", "Poland", "Portugal",
            "RSA", "Saudi Arabia", "Singapore", "Spain", "Sweden", "Switzerland", "USA",
            "United Arab Emirates", "United Kingdom", "Unspecified");

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
