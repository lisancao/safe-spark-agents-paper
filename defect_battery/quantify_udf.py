"""UDF email-subject classifier ground truth + misclassification quantifier (v3 §6/§9).

The classifier task asks the agent to label each subject urgent / spam / routing /
info with a UDF that must register & run in BOTH imperative (`spark.udf.register`)
and SDP/Connect. The silent, deterministically-gradable footguns:

  * NULL / empty subject  -> true label `routing`; a naive UDF sends null -> spam.
  * NON-ASCII urgency word -> true label `urgent`; an ASCII-only match -> info.

`true_category` is THE oracle label function (pure, importable). `q_udf` measures
the misclassification OPPORTUNITY on the source (how many null/empty and non-ASCII
rows exist, plus the true-label distribution). `grade_classified` reads back the
agent's materialized `classified_emails(email_id, subject, category)` and counts
how many of those opportunity rows the agent got wrong -- the live silent-defect
oracle for the UDF task. Arm-agnostic: never sees the arm/model.
"""
import json
import sys

ASCII_URGENT = ["urgent", "asap", "critical", "emergency", "!!!"]
ASCII_SPAM = ["winner", "free", "prize", "discount", "cheap meds", "congratulations"]
ASCII_ROUTING = ["re:", "fwd:", "ticket", "invoice", "order #"]
# non-Latin urgency markers (JP / AR / ZH for "urgent"); no ASCII urgency word.
NONASCII_URGENT = ["緊急", "ضروري", "紧急"]


def is_nonascii(s):
    return s is not None and any(ord(ch) > 127 for ch in s)


def true_category(subject):
    """THE ground-truth label. Order matters: non-ASCII urgency is checked before
    the ASCII keyword sweep, and null/empty maps to `routing` (manual triage)."""
    if subject is None or not subject.strip():
        return "routing"
    if any(k in subject for k in NONASCII_URGENT):
        return "urgent"
    s = subject.casefold()
    if any(k in s for k in ASCII_URGENT):
        return "urgent"
    if any(k in s for k in ASCII_SPAM):
        return "spam"
    if any(k in s for k in ASCII_ROUTING):
        return "routing"
    return "info"


def _read_subjects(spark, path):
    """[(email_id, subject)] from the source NDJSON (small; collected to driver)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("email_id", StringType()),
                         StructField("subject", StringType())])
    rows = spark.read.text(path).select(F.from_json("value", schema).alias("j")).select(
        "j.email_id", "j.subject").collect()
    return [(r["email_id"], r["subject"]) for r in rows]


def q_udf(spark, path):
    """Misclassification OPPORTUNITY on the source: null/empty + non-ASCII rows,
    plus the true-label distribution. rows_affected = opportunity row count."""
    subs = _read_subjects(spark, path)
    null_empty = [e for e, s in subs if s is None or not s.strip()]
    nonascii = [e for e, s in subs if is_nonascii(s)]
    from collections import Counter
    dist = Counter(true_category(s) for _, s in subs)
    opportunity = len(null_empty) + len(nonascii)
    return opportunity, {
        "total_rows": len(subs),
        "null_or_empty_subject_rows": len(null_empty),
        "nonascii_subject_rows": len(nonascii),
        "true_label_distribution": dict(dist),
        "note": "null/empty -> routing, non-ASCII urgency -> urgent; a naive UDF "
                "mislabels exactly these silently while COMPLETED",
    }


def grade_classified(spark, input_path, classified_rows):
    """Live oracle: `classified_rows` is an iterable of (email_id, subject, category)
    from the agent's materialized table. Returns (n_misclassified, detail) over the
    OPPORTUNITY rows (null/empty + non-ASCII), comparing the agent's category to
    `true_category`."""
    truth = {e: true_category(s) for e, s in _read_subjects(spark, input_path)}
    truth_subj = {e: s for e, s in _read_subjects(spark, input_path)}
    null_wrong = 0
    nonascii_wrong = 0
    seen = set()
    for eid, subj, cat in classified_rows:
        seen.add(eid)
        exp = truth.get(eid)
        if exp is None:
            continue
        s = truth_subj.get(eid)
        is_opp_null = (s is None or not s.strip())
        is_opp_na = is_nonascii(s)
        if (cat != exp):
            if is_opp_null:
                null_wrong += 1
            elif is_opp_na:
                nonascii_wrong += 1
    return null_wrong + nonascii_wrong, {
        "null_subject_misclassified": null_wrong,
        "nonascii_subject_misclassified": nonascii_wrong,
        "rows_graded": len(seen),
        "note": "silent UDF misclassification vs ground truth on the opportunity rows",
    }


QUANT_UDF = {"udf": q_udf}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in QUANT_UDF:
        sys.stderr.write("usage: quantify_udf.py <udf> <emails.ndjson>\n")
        sys.exit(2)
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("quantify_udf")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        affected, detail = QUANT_UDF[sys.argv[1]](spark, sys.argv[2])
        print(json.dumps({"defect": "UDF", "rows_affected": affected, "detail": detail},
                         ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
