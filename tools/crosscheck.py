#!/usr/bin/env python3
"""Независимая сверка результатов пайплайна.

Считает те же метрики, что и Java-код, но другой реализацией и без parquet,
прямо по исходному CSV. Нужна, чтобы ошибка в правилах не продублировалась:
проверять Java тем же Java смысла мало.

Правила повторяют org.example.dq.Rules, DefaultReturnPolicy и
StockCodeClassifier. Если в Java меняются правила, скрипт надо править тоже,
иначе сверка начнёт врать.

Запуск:
    python3 tools/crosscheck.py data.csv out
"""

import collections
import csv
import datetime
import decimal
import re
import pathlib
import sys

D = decimal.Decimal
CENT = D("0.01")


def money(value):
    # Деньги округляются вверх от половины, как cast в Spark.
    # Встроенный round() у Decimal округляет к чётному, из-за этого
    # половинные копейки расходились бы с выгрузкой.
    return value.quantize(CENT, rounding=decimal.ROUND_HALF_UP)

PERIOD_START = datetime.date(2010, 12, 1)
PERIOD_END = datetime.date(2011, 12, 9)

SPECIAL_CODES = {
    "POST": "SHIPPING", "DOT": "SHIPPING", "C2": "SHIPPING",
    "M": "ADJUSTMENT", "D": "ADJUSTMENT", "B": "ADJUSTMENT",
    "CRUK": "ADJUSTMENT", "PADS": "ADJUSTMENT",
    "S": "SAMPLE",
    "BANK CHARGES": "FEE", "AMAZONFEE": "FEE",
}


# Товар это цифровой код, возможно с буквенным суффиксом варианта (85123A).
PRODUCT_CODE = re.compile(r"^[0-9]+[A-Za-z]{0,2}$")


def line_kind(stock_code):
    code = stock_code.upper()
    if code in SPECIAL_CODES:
        return SPECIAL_CODES[code]
    if code.startswith("GIFT_"):
        return "GIFT_CARD"
    return "PRODUCT" if PRODUCT_CODE.match(code) else "OTHER"


def read_rows(path):
    with open(path, encoding="latin-1", newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        for record in reader:
            if len(record) != 8:
                continue
            invoice, code, desc, qty, date, price, customer, country = (f.strip() for f in record)
            yield (invoice, code, desc, int(qty),
                   datetime.datetime.strptime(date, "%m/%d/%Y %H:%M"),
                   D(price), customer or None, country)


def canonical_descriptions(rows):
    """Самое частое непустое описание на код, при равенстве - меньшее лексикографически."""
    counts = collections.defaultdict(collections.Counter)
    for invoice, code, desc, *_ in rows:
        if desc and code:
            counts[code][desc] += 1
    return {
        code: max(variants.items(), key=lambda kv: (kv[1], [-ord(ch) for ch in kv[0]]))[0]
        for code, variants in counts.items()
    }


def prepare(rows, canon):
    """Повторяет шаг 2: нормализация, правила ERROR, дедупликация."""
    seen = set()
    for invoice, code, desc, qty, date, price, customer, country in rows:
        desc = canon.get(code, desc)
        if not invoice or not code or not country or qty == 0 or price < 0:
            continue
        if not PERIOD_START <= date.date() <= PERIOD_END:
            continue
        fingerprint = (invoice, code, desc, qty, date, price, customer, country)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        yield fingerprint


def top_products(prepared, limit=5):
    stats = collections.defaultdict(lambda: {"sold": 0, "returned": 0, "revenue": D(0), "desc": ""})
    for invoice, code, desc, qty, date, price, customer, country in prepared:
        if line_kind(code) != "PRODUCT":
            continue
        stat = stats[code]
        if qty < 0:
            stat["returned"] += -qty
        else:
            stat["sold"] += qty
        stat["revenue"] += price * qty
        if not stat["desc"]:
            stat["desc"] = desc
    # Товар, которого вернули больше, чем купили, в рейтинг популярности не идёт.
    positive = {code: stat for code, stat in stats.items() if stat["sold"] - stat["returned"] > 0}
    ranked = sorted(positive.items(), key=lambda kv: (-(kv[1]["sold"] - kv[1]["returned"]), kv[0]))
    return ranked[:limit]


def customer_metrics(prepared):
    stats = collections.defaultdict(lambda: {"sales": set(), "all": set(), "gross": D(0), "returns": D(0)})
    for invoice, code, desc, qty, date, price, customer, country in prepared:
        if customer is None:
            continue
        stat = stats[customer]
        stat["all"].add(invoice)
        if qty < 0:
            stat["returns"] += -(price * qty)
        else:
            stat["sales"].add(invoice)
            stat["gross"] += price * qty
    return stats


def compare(name, expected, actual):
    status = "ок" if expected == actual else "РАСХОЖДЕНИЕ"
    print(f"{status}: {name}")
    if expected != actual:
        print(f"  java:   {actual}")
        print(f"  python: {expected}")
    return expected == actual


def main():
    source = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "data.csv")
    out_dir = pathlib.Path(sys.argv[2] if len(sys.argv) > 2 else "out")

    rows = list(read_rows(source))
    prepared = list(prepare(rows, canonical_descriptions(rows)))
    print(f"строк в источнике: {len(rows)}, принято: {len(prepared)}")

    ok = True

    report = {row["Rule"]: row["Rows"]
              for row in csv.DictReader(open(out_dir / "dq_report.csv", encoding="utf-8"))}
    ok &= compare("число принятых строк", str(len(prepared)), report.get("rows.accepted"))

    java_top = list(csv.reader(open(out_dir / "top_products.csv", encoding="utf-8")))[1:]
    for (code, stat), java_row in zip(top_products(prepared), java_top):
        net = stat["sold"] - stat["returned"]
        expected = [code, stat["desc"], str(stat["sold"]), str(stat["returned"]), str(net),
                    str(money(stat["revenue"]))]
        ok &= compare(f"топ товара {java_row[0]}", expected, java_row[1:])

    customers = customer_metrics(prepared)
    java_customers = {row[0]: row[1:] for row in
                      list(csv.reader(open(out_dir / "customer_metrics.csv", encoding="utf-8")))[1:]}
    ok &= compare("число клиентов", str(len(customers)), str(len(java_customers)))

    for customer, stat in sorted(customers.items(),
                                 key=lambda kv: -(kv[1]["gross"] - kv[1]["returns"]))[:20]:
        net = stat["gross"] - stat["returns"]
        expected = [str(len(stat["sales"])), str(len(stat["all"])),
                    str(money(stat["gross"])), str(money(stat["returns"])), str(money(net)),
                    str(money(stat["gross"] / len(stat["sales"]))),
                    str(money(net / len(stat["all"])))]
        ok &= compare(f"клиент {customer}", expected, java_customers.get(customer))

    print()
    print("ИТОГ: всё сходится" if ok else "ИТОГ: ЕСТЬ РАСХОЖДЕНИЯ")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
