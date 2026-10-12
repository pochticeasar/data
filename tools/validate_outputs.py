#!/usr/bin/env python3
"""Проверка выгрузок пайплайна: типы, инварианты, соответствие схеме датасета.

Отвечает на вопрос "можно ли отдавать эти CSV дальше". Три группы проверок:

1. Типы и форматы: целое там, где целое, две цифры после запятой в деньгах,
   пустых ячеек нет.
2. Инварианты внутри файла: QuantityNet = Sold - Returned, TotalNet =
   Gross - Returns, средний чек = сумма / число заказов, сортировка.
3. Схема источника (описание датасета на UCI): InvoiceNo 6 цифр или C + 6 цифр,
   StockCode 5 цифр, CustomerID 5 цифр. Отклонения в выгрузках печатаются
   как предупреждения: в источнике они массовые и это нормально для этих данных.

Запуск:
    python3 tools/validate_outputs.py out
"""

import csv
import decimal
import pathlib
import re
import sys

D = decimal.Decimal
CENT = D("0.01")


def money(value):
    # Деньги округляются вверх от половины, как cast в Spark.
    # Встроенный round() у Decimal округляет к чётному, из-за этого
    # половинные копейки расходились бы с выгрузкой.
    return value.quantize(CENT, rounding=decimal.ROUND_HALF_UP)

INT = re.compile(r"^-?\d+$")
MONEY = re.compile(r"^-?\d+\.\d{2}$")
MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
INVOICE = re.compile(r"^[Cc]?\d{6}$")
STOCK = re.compile(r"^\d{5}$")
STOCK_VARIANT = re.compile(r"^\d{5}[A-Za-z]{1,2}$")
CUSTOMER = re.compile(r"^\d{5}$")

DIMENSIONS = {"TOTAL", "SCHEMA", "COMPLETENESS", "VALIDITY", "CONSISTENCY", "UNIQUENESS", "REFERENCE"}
SEVERITIES = {"INFO", "WARNING", "ERROR"}

failures = []
warnings = []


def check(condition, message):
    if not condition:
        failures.append(message)
    return condition


def warn(condition, message):
    if not condition:
        warnings.append(message)


def read(path):
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows


def check_header(path, expected):
    with open(path, encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    check(header == expected, f"{path.name}: заголовок {header} вместо {expected}")


def check_types(path, rows, int_columns=(), money_columns=()):
    for number, row in enumerate(rows, start=2):
        for column in int_columns:
            check(INT.match(row[column] or ""),
                  f"{path.name}:{number} колонка {column} не целое: {row[column]!r}")
        for column in money_columns:
            check(MONEY.match(row[column] or ""),
                  f"{path.name}:{number} колонка {column} не денежная величина: {row[column]!r}")


def check_top_products(out):
    path = out / "top_products.csv"
    check_header(path, ["Rank", "StockCode", "Description", "QuantitySold",
                        "QuantityReturned", "QuantityNet", "RevenueNet"])
    rows = read(path)
    check(len(rows) == 5, f"{path.name}: {len(rows)} строк вместо 5")
    check_types(path, rows,
                int_columns=("Rank", "QuantitySold", "QuantityReturned", "QuantityNet"),
                money_columns=("RevenueNet",))

    previous = None
    for number, row in enumerate(rows, start=2):
        check(int(row["Rank"]) == number - 1, f"{path.name}:{number} Rank не по порядку")
        check(row["Description"].strip() != "", f"{path.name}:{number} пустое Description")
        net = int(row["QuantitySold"]) - int(row["QuantityReturned"])
        check(net == int(row["QuantityNet"]),
              f"{path.name}:{number} QuantityNet {row['QuantityNet']} != {net}")
        if previous is not None:
            check(int(row["QuantityNet"]) <= previous,
                  f"{path.name}:{number} нарушена сортировка по QuantityNet")
        previous = int(row["QuantityNet"])
        code = row["StockCode"]
        if not STOCK.match(code):
            warn(bool(STOCK_VARIANT.match(code)),
                 f"{path.name}:{number} StockCode {code!r} не похож на код товара")
            warnings.append(f"{path.name}:{number} StockCode {code!r} отклоняется от схемы "
                            f"(ожидается 5 цифр, это вариант товара)")
    print(f"ок: {path.name} ({len(rows)} строк)")


def check_customer_metrics(out):
    path = out / "customer_metrics.csv"
    check_header(path, ["CustomerID", "OrdersSales", "OrdersTotal", "TotalGross",
                        "ReturnsAmount", "TotalNet", "AvgCheckGross", "AvgCheckNet"])
    rows = read(path)
    check_types(path, rows,
                int_columns=("OrdersSales", "OrdersTotal"),
                money_columns=("TotalGross", "ReturnsAmount", "TotalNet",
                               "AvgCheckGross", "AvgCheckNet"))

    seen = set()
    previous = None
    only_returns = 0
    for number, row in enumerate(rows, start=2):
        customer = row["CustomerID"]
        check(customer not in seen, f"{path.name}:{number} клиент {customer} встречается дважды")
        seen.add(customer)
        warn(bool(CUSTOMER.match(customer)),
             f"{path.name}:{number} CustomerID {customer!r} не 5 цифр, схема это запрещает")

        sales, total = int(row["OrdersSales"]), int(row["OrdersTotal"])
        check(sales <= total, f"{path.name}:{number} OrdersSales {sales} > OrdersTotal {total}")
        check(total > 0, f"{path.name}:{number} OrdersTotal = 0")

        gross, returns, net = D(row["TotalGross"]), D(row["ReturnsAmount"]), D(row["TotalNet"])
        check(gross - returns == net,
              f"{path.name}:{number} TotalNet {net} != {gross} - {returns}")
        check(returns >= 0, f"{path.name}:{number} отрицательная сумма возвратов")

        expected_gross = money(D(0)) if sales == 0 else money(gross / sales)
        check(D(row["AvgCheckGross"]) == expected_gross,
              f"{path.name}:{number} AvgCheckGross {row['AvgCheckGross']} != {expected_gross}")
        check(D(row["AvgCheckNet"]) == money(net / total),
              f"{path.name}:{number} AvgCheckNet {row['AvgCheckNet']} != {money(net / total)}")

        if sales == 0:
            only_returns += 1
        if previous is not None:
            check(net <= previous, f"{path.name}:{number} нарушена сортировка по TotalNet")
        previous = net

    print(f"ок: {path.name} ({len(rows)} клиентов, из них только с возвратами: {only_returns})")
    return rows


def check_monthly(out):
    path = out / "monthly_revenue.csv"
    check_header(path, ["Month", "Orders", "RevenueSales", "Returns", "RevenueNet", "ReturnsSharePct"])
    rows = read(path)
    check_types(path, rows, int_columns=("Orders",),
                money_columns=("RevenueSales", "Returns", "RevenueNet", "ReturnsSharePct"))

    previous = None
    for number, row in enumerate(rows, start=2):
        check(bool(MONTH.match(row["Month"])), f"{path.name}:{number} месяц {row['Month']!r}")
        if previous is not None:
            check(row["Month"] > previous, f"{path.name}:{number} месяцы не по порядку")
        previous = row["Month"]

        sales, returns, net = D(row["RevenueSales"]), D(row["Returns"]), D(row["RevenueNet"])
        check(sales - returns == net, f"{path.name}:{number} RevenueNet {net} != {sales} - {returns}")
        expected_share = money(returns * 100 / sales) if sales else money(D(0))
        check(D(row["ReturnsSharePct"]) == expected_share,
              f"{path.name}:{number} ReturnsSharePct {row['ReturnsSharePct']} != {expected_share}")
        check(int(row["Orders"]) > 0, f"{path.name}:{number} ноль заказов в месяце")

    print(f"ок: {path.name} ({len(rows)} месяцев)")
    return rows


def check_report(out):
    path = out / "dq_report.csv"
    check_header(path, ["Rule", "Dimension", "Severity", "Rows"])
    rows = read(path)
    check_types(path, rows, int_columns=("Rows",))

    values = {row["Rule"]: int(row["Rows"]) for row in rows}
    for row in rows:
        check(row["Dimension"] in DIMENSIONS, f"{path.name}: измерение {row['Dimension']!r}")
        check(row["Severity"] in SEVERITIES, f"{path.name}: серьёзность {row['Severity']!r}")

    read_rows = values.get("rows.read")
    accepted = values.get("rows.accepted")
    rejected = values.get("rows.rejected")
    check(None not in (read_rows, accepted, rejected), f"{path.name}: нет итоговых счётчиков")
    if None not in (read_rows, accepted, rejected):
        check(read_rows == accepted + rejected,
              f"{path.name}: баланс не сходится: {read_rows} != {accepted} + {rejected}")
    print(f"ок: {path.name} (правил сработало: {len(rows) - 3})")
    return values


def check_rejected(out, report):
    for name, counter in (("rejected.csv", "rows.rejected"), ("rejected_ingest.csv", None)):
        path = out / name
        check_header(path, ["LineNumber", "Rule", "Payload"])
        rows = read(path)
        check_types(path, rows, int_columns=("LineNumber",))
        for number, row in enumerate(rows, start=2):
            check(row["Rule"].strip() != "", f"{path.name}:{number} пустое правило")
        if counter and counter in report:
            check(len(rows) == report[counter],
                  f"{path.name}: {len(rows)} строк, а в отчёте {report[counter]}")
        print(f"ок: {path.name} ({len(rows)} строк)")


def check_cross_file(customers, months):
    customer_net = sum(D(row["TotalNet"]) for row in customers)
    monthly_net = sum(D(row["RevenueNet"]) for row in months)
    check(customer_net <= monthly_net,
          f"выручка по клиентам {customer_net} больше выручки по месяцам {monthly_net}")
    print(f"ок: netto по клиентам {customer_net} <= netto по месяцам {monthly_net} "
          f"(разница {monthly_net - customer_net} это строки без CustomerID)")


def main():
    out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "out")
    check_top_products(out)
    customers = check_customer_metrics(out)
    months = check_monthly(out)
    report = check_report(out)
    check_rejected(out, report)
    check_cross_file(customers, months)

    if warnings:
        print(f"\nпредупреждения ({len(warnings)}), отклонения от схемы источника:")
        for message in warnings[:10]:
            print(f"  {message}")
        if len(warnings) > 10:
            print(f"  ... ещё {len(warnings) - 10}")

    print()
    if failures:
        print(f"ИТОГ: {len(failures)} проверок не прошло")
        for message in failures[:20]:
            print(f"  {message}")
        return 1
    print("ИТОГ: все выгрузки корректны")
    return 0


if __name__ == "__main__":
    sys.exit(main())
