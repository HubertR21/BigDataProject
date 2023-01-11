# %%
crypto_symbols = [
    "BTCUSDT",
    "BTCEUR",
    "BTCGBP",
    "ETHUSDT",
    "ETHEUR",
    "ETHGBP",
    "DOGEUSDT",
    "DOGEEUR",
    "DOGEGBP",
    "ADAUSDT",
    "ADAEUR",
    "ADAGBP",
    "DOTUSDT",
    "DOTEUR",
    "DOTGBP",
    "MATICUSDT",
    "MATICEUR",
    "MATICGBP",
    "XRPUSDT",
    "XRPEUR",
    "XRPGBP",
]
crypto_symbols_map = {
    "BTCUSDT": ("BTC", "USDT"),
    "BTCEUR": ("BTC", "EUR"),
    "BTCGBP": ("BTC", "GBP"),
    "ETHUSDT": ("ETH", "USDT"),
    "ETHEUR": ("ETH", "EUR"),
    "ETHGBP": ("ETH", "GBP"),
    "DOGEUSDT": ("DOGE", "USDT"),
    "DOGEEUR": ("DOGE", "EUR"),
    "DOGEGBP": ("DOGE", "GBP"),
    "ADAUSDT": ("ADA", "USDT"),
    "ADAEUR": ("ADA", "EUR"),
    "ADAGBP": ("ADA", "GBP"),
    "DOTUSDT": ("DOT", "USDT"),
    "DOTEUR": ("DOT", "EUR"),
    "DOTGBP": ("DOT", "GBP"),
    "MATICUSDT": ("MATIC", "USDT"),
    "MATICEUR": ("MATIC", "EUR"),
    "MATICGBP": ("MATIC", "GBP"),
    "XRPUSDT": ("XRP", "USDT"),
    "XRPEUR": ("XRP", "EUR"),
    "XRPGBP": ("XRP", "GBP"),
}
# %%
import findspark
 
findspark.init()
import sys
import subprocess
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType
 
formatter = logging.Formatter(
    "[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s"
)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
# %%
 
app_name = "spark_projekt"
master = "local"
 
spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
# %%
twitter_schema = (
    StructType()
    .add("end", "date")
    .add("start", "date")
    .add("tweet_count", "integer")
    .add("time", "string")
)
twitter_data = spark.read.csv(
    "hdfs://localhost:8020/user/testowanie/twitter_*.csv",
    header=True,
    schema=twitter_schema,
)
 
# %%
 
crypto_schema = (
    StructType().add("symbol", "string").add("price", "float").add("time", "string")
)
crypto_data = spark.read.csv(
    "hdfs://localhost:8020/user/testowanie/crypto_*.csv",
    header=True,
    schema=crypto_schema,
)
 
# %%
print('saving hive tables')
twitter_data.write.partitionBy("time").saveAsTable("twitter_data_test", mode="overwrite")
crypto_data.write.partitionBy("time").saveAsTable("crypto_data_test", mode="overwrite")
 
# %%
spark.sql("select * from twitter_data_test").show()
spark.sql("select * from crypto_data_test").show()
 
# %%
print('import happybase')
import happybase
print('import tqdm')
from tqdm import tqdm
 
connection = happybase.Connection("localhost", port=9090)
# %%
print('creating project table')
if b"projekt_test" not in connection.tables():
    connection.create_table(
        "projekt_test",
        {
            "symbols": dict(max_versions=1, block_cache_enabled=False),
            "price_data": dict(max_versions=1, block_cache_enabled=False),
        },
    )
print('populating the project table')
table = connection.table("projekt_test")
# %%
for i, cd in tqdm(enumerate(crypto_data.collect()), total=crypto_data.count()):
    symbol_1, symbol_2 = crypto_symbols_map[cd.symbol]
 
    table.put(
        f"crypto_data_{i}",
        {
            "symbols:trade_symbol": cd.symbol,
            "symbols:symbol_1": symbol_1,
            "symbols:symbol_2": symbol_2,
            "price_data:price": str(cd.price),
            "price_data:time": cd.time,
        },
    )
 