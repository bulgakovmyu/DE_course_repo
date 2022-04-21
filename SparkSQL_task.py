# %%
# import packages
from IPython.display import display
from numpy import double

from utils.spark_util import read_config, AzureConnector, AsureReader
from src.data_prepare import SparkSQL_hw

from vars.paths import source

# %%
# load configs and creds and initialize session
config = read_config("config.yml")["azure"]
session = AzureConnector(config=config).get_session()
# %%
# load the data
hotels_weather = AsureReader(
    session=session, data_path=f"{source}hotel-weather", file_format="parquet"
).read()
expedia = AsureReader(
    session=session, data_path=f"{source}expedia", file_format="avro"
).read()
# %%
# look at the data
print("hotels_weather>>>>:")
display(hotels_weather.limit(5).toPandas())
print("expedia>>>>:")
display(expedia.limit(5).toPandas())

# %%
# Initialize the task class
task_class = SparkSQL_hw(hotels_weather_df=hotels_weather, expedia_df=expedia)
# %%
# Task1
task_1_res = task_class.get_top10_with_max_month_temp_diff().toPandas()
print("Task1 result>>>>:")
display(task_1_res)

# %%
task_2_res = task_class.get_top_10_popular_hotels_by_month().toPandas()
print("Task2 result>>>>:")
display(task_2_res)

# %%
task_3_res = task_class.get_aggregations_for_long_stays().toPandas()
print("Task3 result>>>>:")
display(task_3_res)

# %%
