"""On-chain features per token per day (PySpark), from token_transfers_raw + address labels.

Wash trading = activity that inflates volume without new real participants. The
features below measure that from several angles. Following the Phase 2 finding,
round-trip trading is counted between WALLETS only: DEX pools, routers and bots
trade back and forth with everyone by design and would otherwise dominate.

Rolling statistics are trailing and exclude the current day, as in market.py.
The first and last day of the pull window are partial and are dropped.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

WINDOW_DAYS = 14      # the on-chain pull covers 90 days, so a shorter baseline than market's 30
MIN_HISTORY = 10
ROUND_TRIP_DAYS = 7
HUB_DEGREE = 1_000    # distinct counterparties that mark an address as infrastructure-like
LARGE_QUANTILE = 0.99
EPS = 1e-12


def round_trip_legs(transfers: DataFrame, labels: DataFrame, days: int = ROUND_TRIP_DAYS) -> DataFrame:
    """Wallet-to-wallet transfers a->b where b->a (same token) happened within the previous `days`.

    Returns the qualifying transfers (token, tx_hash, tx_seq) with their amount and day.
    Trailing only: a transfer is not marked by a reply that comes later.
    """
    wallets = labels.where(~F.col("is_contract")).select(F.col("address"))
    ww = (transfers
          .join(wallets.withColumnRenamed("address", "from_address"), "from_address")
          .join(wallets.withColumnRenamed("address", "to_address"), "to_address")
          .where(F.col("from_address") != F.col("to_address"))
          .select("token", "tx_hash", "tx_seq", "block_time", "from_address", "to_address", "amount"))
    prior = ww.select(F.col("token").alias("p_token"), F.col("from_address").alias("p_from"),
                      F.col("to_address").alias("p_to"), F.col("block_time").alias("p_time"))
    window_secs = days * 86_400
    legs = (ww.join(prior,
                    (F.col("token") == F.col("p_token"))
                    & (F.col("from_address") == F.col("p_to"))
                    & (F.col("to_address") == F.col("p_from"))
                    & (F.col("p_time") <= F.col("block_time"))
                    & (F.unix_timestamp("p_time") >= F.unix_timestamp("block_time") - window_secs))
              .select("token", "tx_hash", "tx_seq", "block_time", "from_address", "to_address", "amount")
              .dropDuplicates(["token", "tx_hash", "tx_seq"]))
    return legs.withColumn("day", F.to_date("block_time"))


def hub_addresses(transfers: DataFrame, min_degree: int = HUB_DEGREE) -> DataFrame:
    """Addresses with >= min_degree distinct counterparties for a token (pools, routers, exchanges)."""
    edges = (transfers.select("token", F.col("from_address").alias("address"), F.col("to_address").alias("other"))
             .unionByName(transfers.select("token", F.col("to_address").alias("address"),
                                           F.col("from_address").alias("other"))))
    return (edges.groupBy("token", "address").agg(F.countDistinct("other").alias("degree"))
                 .where(F.col("degree") >= min_degree).select("token", "address"))


def _z(value: Column, mean: Column, std: Column) -> Column:
    return F.when(std > EPS, (value - mean) / std)


def daily_activity(transfers: DataFrame, labels: DataFrame) -> DataFrame:
    """Raw daily counts per token (before rolling statistics)."""
    t = transfers.withColumn("day", F.to_date("block_time"))

    # large transfer threshold: the token's own 99th percentile over the window. A fixed
    # per-token cut (slight lookahead) keeps "large" comparable across days.
    thresholds = t.groupBy("token").agg(F.percentile_approx("amount", LARGE_QUANTILE, 10_000).alias("large_cut"))
    t = t.join(thresholds, "token")

    hubs = hub_addresses(transfers).withColumn("is_hub", F.lit(True))
    t = (t.join(hubs.withColumnRenamed("address", "from_address").withColumnRenamed("is_hub", "from_hub"),
                ["token", "from_address"], "left")
          .join(hubs.withColumnRenamed("address", "to_address").withColumnRenamed("is_hub", "to_hub"),
                ["token", "to_address"], "left"))

    first_seen = (t.select("token", "day", F.explode(F.array("from_address", "to_address")).alias("address"))
                   .groupBy("token", "address").agg(F.min("day").alias("day"))
                   .groupBy("token", "day").agg(F.count("*").alias("new_addresses")))

    sender_amounts = t.groupBy("token", "day", "from_address").agg(F.sum("amount").alias("sent"))
    top10 = (sender_amounts
             .withColumn("rank", F.row_number().over(Window.partitionBy("token", "day").orderBy(F.desc("sent"))))
             .groupBy("token", "day").agg(F.sum(F.when(F.col("rank") <= 10, F.col("sent"))).alias("top10_sent"),
                                          F.sum("sent").alias("all_sent"))
             .select("token", "day", (F.col("top10_sent") / F.col("all_sent")).alias("top10_sender_share")))

    legs = round_trip_legs(transfers, labels)
    rt = legs.groupBy("token", "day").agg(
        F.count("*").alias("rt_transfers"),
        F.sum("amount").alias("rt_amount"),
        F.countDistinct(F.least("from_address", "to_address"), F.greatest("from_address", "to_address")).alias("rt_pairs"))

    base = t.groupBy("token", "day").agg(
        F.count("*").alias("transfers"),
        F.sum("amount").alias("amount_total"),
        F.countDistinct("from_address").alias("unique_senders"),
        F.countDistinct("to_address").alias("unique_receivers"),
        F.size(F.array_union(F.collect_set("from_address"), F.collect_set("to_address"))).alias("unique_addresses"),
        F.sum(F.when(F.col("amount") > F.col("large_cut"), 1).otherwise(0)).alias("large_transfers"),
        F.avg(F.when(F.col("from_hub") | F.col("to_hub"), 1.0).otherwise(0.0)).alias("hub_share"))

    out = (base.join(first_seen, ["token", "day"], "left")
               .join(top10, ["token", "day"], "left")
               .join(rt, ["token", "day"], "left")
               .fillna(0, ["new_addresses", "rt_transfers", "rt_amount", "rt_pairs"]))
    return (out.withColumn("rt_share", F.col("rt_transfers") / F.col("transfers"))
               .withColumn("rt_amount_share", F.when(F.col("amount_total") > 0,
                                                     F.col("rt_amount") / F.col("amount_total")).otherwise(0.0))
               .withColumn("transfers_per_address", F.col("transfers") / F.col("unique_addresses"))
               .withColumn("new_address_share", F.col("new_addresses") / F.col("unique_addresses")))


def add_onchain_features(daily: DataFrame, window_days: int = WINDOW_DAYS,
                         min_history: int = MIN_HISTORY) -> DataFrame:
    """Drop the partial edge days, then add trailing z-scores per token."""
    bounds = daily.groupBy("token").agg(F.min("day").alias("_first"), F.max("day").alias("_last"))
    d = (daily.join(bounds, "token")
              .where((F.col("day") > F.col("_first")) & (F.col("day") < F.col("_last")))
              .drop("_first", "_last")
              .withColumn("_n", F.datediff("day", F.lit("1970-01-01")))
              .withColumn("log_transfers", F.log1p("transfers"))
              .withColumn("log_amount", F.log1p("amount_total"))
              .withColumn("log_addresses", F.log1p("unique_addresses")))
    w = Window.partitionBy("token").orderBy("_n").rangeBetween(-window_days, -1)
    d = d.withColumn("n_history", F.count("transfers").over(w))
    for src, name in [("log_transfers", "transfers_z"), ("log_amount", "amount_z"),
                      ("log_addresses", "addresses_z"), ("rt_share", "rt_share_z"),
                      ("transfers_per_address", "tpa_z"), ("top10_sender_share", "concentration_z"),
                      ("large_transfers", "large_transfers_z")]:
        d = d.withColumn(name, F.when(F.col("n_history") >= min_history,
                                      _z(F.col(src), F.avg(src).over(w), F.stddev_samp(src).over(w))))
    # wash-trading signature: activity growing faster than the set of participants
    d = d.withColumn("activity_vs_participants", F.col("transfers_z") - F.col("addresses_z"))
    return d.drop("_n", "log_transfers", "log_amount", "log_addresses")
