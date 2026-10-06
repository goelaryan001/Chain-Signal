"""Market features per coin per day (PySpark), from market_daily_clean.

Every rolling statistic is TRAILING and excludes the current day
(rangeBetween(-window, -1) over calendar day numbers). A spike must be measured
against what came before it; including it in its own baseline would partly hide
it (lookahead leakage). Calendar-day ranges also handle missing days correctly.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

WINDOW_DAYS = 30
MIN_HISTORY = 20   # trailing observations required before any feature is emitted
EPS = 1e-12


def _z(value: Column, mean: Column, std: Column) -> Column:
    return F.when(std > EPS, (value - mean) / std)


def market_return(df: DataFrame) -> DataFrame:
    """Daily market return = median log return across clean coins.

    Median rather than cap-weighted: a cap-weighted index would be mostly BTC, ETH and
    stablecoins, and the median is robust to the remaining single-day extreme moves.
    """
    return (df.where(F.col("log_return").isNotNull() & ~F.col("extreme_move"))
              .groupBy("date").agg(F.median("log_return").alias("market_return")))


def add_market_features(df: DataFrame, window_days: int = WINDOW_DAYS,
                        min_history: int = MIN_HISTORY) -> DataFrame:
    """Input columns: coin_id, date, price, market_cap, volume, log_return, extreme_move,
    missing_days_before. Output: one row per input row with the features added."""
    d = (df
         # a return spanning missing days is a multi-day return, not comparable to the rest
         .withColumn("log_return", F.when(F.coalesce(F.col("missing_days_before"), F.lit(0)) == 0,
                                          F.col("log_return")))
         .withColumn("day", F.datediff("date", F.lit("1970-01-01")))
         .withColumn("log_volume", F.log1p("volume"))
         .withColumn("log_turnover", F.when((F.col("market_cap") > 0) & (F.col("volume") > 0),
                                            F.log(F.col("volume") / F.col("market_cap")))))
    d = d.join(market_return(d), "date", "left")

    w = Window.partitionBy("coin_id").orderBy("day").rangeBetween(-window_days, -1)
    x, y = F.col("log_return"), F.when(F.col("log_return").isNotNull(), F.col("market_return"))
    d = (d
         .withColumn("n_history", F.count(x).over(w))
         .withColumn("ret_mean", F.avg(x).over(w))
         .withColumn("ret_std", F.stddev_samp(x).over(w))
         .withColumn("lv_mean", F.avg("log_volume").over(w))
         .withColumn("lv_std", F.stddev_samp("log_volume").over(w))
         .withColumn("lt_mean", F.avg("log_turnover").over(w))
         .withColumn("lt_std", F.stddev_samp("log_turnover").over(w))
         # rolling correlation / beta vs the market from trailing moments
         .withColumn("_ex", F.avg(x).over(w)).withColumn("_ey", F.avg(y).over(w))
         .withColumn("_exy", F.avg(x * y).over(w))
         .withColumn("_exx", F.avg(x * x).over(w)).withColumn("_eyy", F.avg(y * y).over(w)))
    cov = F.col("_exy") - F.col("_ex") * F.col("_ey")
    var_x = F.col("_exx") - F.col("_ex") ** 2
    var_y = F.col("_eyy") - F.col("_ey") ** 2
    d = (d
         .withColumn("market_corr", F.when((var_x > EPS) & (var_y > EPS), cov / F.sqrt(var_x * var_y)))
         .withColumn("market_beta", F.when(var_y > EPS, cov / var_y))
         .withColumn("return_z", _z(x, F.col("ret_mean"), F.col("ret_std")))
         .withColumn("volume_z", _z(F.col("log_volume"), F.col("lv_mean"), F.col("lv_std")))
         .withColumn("turnover_z", _z(F.col("log_turnover"), F.col("lt_mean"), F.col("lt_std")))
         .withColumn("residual_return", x - F.col("market_beta") * F.col("market_return"))
         # idiosyncratic volatility = total volatility * sqrt(1 - rho^2) under a one-factor model
         .withColumn("_idio_std", F.col("ret_std") * F.sqrt(F.greatest(F.lit(0.0), 1 - F.col("market_corr") ** 2)))
         .withColumn("residual_z", F.when(F.col("_idio_std") > EPS,
                                          F.col("residual_return") / F.col("_idio_std")))
         # wash-trading signature: abnormal volume without an abnormal price move
         .withColumn("volume_price_gap", F.col("volume_z") - F.abs(F.col("return_z"))))

    feature_cols = ["return_z", "volume_z", "turnover_z", "market_corr", "market_beta",
                    "residual_return", "residual_z", "volume_price_gap"]
    enough = F.col("n_history") >= min_history
    for c in feature_cols:
        d = d.withColumn(c, F.when(enough, F.col(c)))
    return d.select("coin_id", "date", "price", "market_cap", "volume", "log_return", "extreme_move",
                    "market_return", "n_history", *feature_cols)
