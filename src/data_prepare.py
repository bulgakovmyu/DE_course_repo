from pyspark.sql.functions import (
    count,
    col,
    min,
    max,
    desc,
    abs,
    to_date,
    trunc,
    months_between,
    col,
    add_months,
    count,
    desc,
    asc,
    rank,
    datediff,
    last,
    first,
    avg,
)
import pyspark

from pyspark.sql.window import Window
from vars.task3 import long_stay_trshld


class SparkSQL_hw(object):
    """
    Class to get aggreagtions for Spark SQL HW
    Input:
        - hotels_weather_df - dataframe with weather info about hotels
        - expedia_df - dataframe with hotel searching data
    """

    def __init__(
        self,
        hotels_weather_df: pyspark.sql.dataframe.DataFrame,
        expedia_df: pyspark.sql.dataframe.DataFrame,
    ):
        self.hotels_weather_df = hotels_weather_df
        self.expedia_df = expedia_df

    def get_top10_with_max_month_temp_diff(self):
        """
        Method to get the data about
        top 10 hotels with max absolute temperature difference by month
        """
        return (
            self.hotels_weather_df.groupBy("id", "name", "year", "month")
            .agg(abs(max("avg_tmpr_c") - min("avg_tmpr_c")).alias("monthly_diff"))
            .groupBy("id", "name")
            .agg(max("monthly_diff").alias("max_monthly_diff"))
            .orderBy(desc("max_monthly_diff"))
            .limit(10)
        )

    def get_top_10_popular_hotels_by_month(self):
        """
        Method to get the data about
        top 10 popular hotels for each month.

        We devide our dataset on two parts - where the only one month is in stay dates and others.
        The first group processed simply with groupby
        The second group processed iteratively (look method "transform_to_all_months")
        """
        expedia_with_months = (
            self.expedia_df.withColumn(
                "srch_ci_month", trunc(to_date(col("srch_ci")), "month")
            )
            .withColumn("srch_co_month", trunc(to_date(col("srch_co")), "month"))
            .withColumn(
                "monthDiff",
                months_between(col("srch_co_month"), col("srch_ci_month")).cast("Int"),
            )
        )

        part_1 = (
            expedia_with_months.filter(expedia_with_months["monthDiff"] == 0)
            .groupBy("hotel_id", "srch_ci_month")
            .agg(count("id").alias("count_search_requests"))
            .withColumnRenamed("srch_ci_month", "srch_month")
        )

        part_2 = (
            self.transform_to_all_months(expedia_with_months)
            .groupBy("hotel_id", "srch_month")
            .sum("count_search_requests")
            .withColumnRenamed("sum(count_search_requests)", "count_search_requests")
        )

        result = (
            part_1.union(part_2)
            .groupBy("srch_month", "hotel_id")
            .sum("count_search_requests")
            .withColumnRenamed("sum(count_search_requests)", "count_search_requests")
            .orderBy(desc("count_search_requests"))
        )

        return self.get_top10_from_groups(
            result, partition_by="srch_month", order_by="count_search_requests", top=10
        )

    def get_aggregations_for_long_stays(self):
        """
        Method to get the data about
        weather trend and average temperature during stay for long stay searches
        """
        expedia_long_stays = self.expedia_df.filter(
            (datediff(col("srch_co"), col("srch_ci")).cast("Int"))
            > long_stay_trshld - 1,
        ).select("id", "hotel_id", "srch_ci", "srch_co")

        hotels_weather_red = self.hotels_weather_df.select(
            col("id"), col("wthr_date"), col("avg_tmpr_c")
        ).withColumnRenamed("id", "hotel_id_key")

        joined_df = (
            expedia_long_stays.join(
                hotels_weather_red,
                expedia_long_stays.hotel_id == hotels_weather_red.hotel_id_key,
                how="left",
            )
            .filter(
                (to_date(col("wthr_date")) >= to_date(col("srch_ci")))
                & (to_date(col("wthr_date")) <= to_date(col("srch_co")))
            )
            .drop("hotel_id_key")
            .orderBy(desc("hotel_id"), asc("wthr_date"))
        )

        return joined_df.groupBy("id", "hotel_id", "srch_ci", "srch_co").agg(
            (last(col("avg_tmpr_c")) - first(col("avg_tmpr_c"))).alias("trend"),
            avg(col("avg_tmpr_c")).alias("average"),
        )

    @staticmethod
    def transform_to_all_months(expedia_with_months: pyspark.sql.dataframe.DataFrame):
        expedia_with_months_buf = expedia_with_months.filter(
            expedia_with_months["monthDiff"] > 0
        )

        up = (
            expedia_with_months_buf.groupBy("hotel_id", "srch_ci_month")
            .agg(count("id").alias("count_search_requests"))
            .withColumnRenamed("srch_ci_month", "srch_month")
        )
        max_month_diff = expedia_with_months_buf.agg({"monthDiff": "max"}).collect()[0][
            0
        ]

        for i in range(max_month_diff + 1):
            expedia_with_months_buf = expedia_with_months_buf.filter(
                expedia_with_months_buf["monthDiff"] > i
            ).withColumn("srch_cur_month", add_months("srch_ci_month", i + 1))

            down = (
                expedia_with_months_buf.groupBy("hotel_id", "srch_cur_month")
                .agg(count("id").alias("count_search_requests"))
                .withColumnRenamed("srch_cur_month", "srch_month")
            )
            up = up.union(down)

            expedia_with_months_buf = expedia_with_months_buf.drop(
                expedia_with_months_buf.srch_cur_month
            )

        return up

    @staticmethod
    def get_top10_from_groups(
        df: pyspark.sql.dataframe.DataFrame, partition_by: str, order_by: str, top: int
    ):

        window = Window.partitionBy(df[partition_by]).orderBy(df[order_by].desc())

        return (
            df.select("*", rank().over(window).alias("rank"))
            .filter(col("rank") <= top)
            .orderBy(asc("srch_month"), desc("count_search_requests"))
            .drop("rank")
        )
