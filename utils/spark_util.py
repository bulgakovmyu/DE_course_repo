import yaml
from pyspark.sql import SparkSession
import pyspark
from pyspark import SparkConf


def read_config(file):
    with open(file, "r") as stream:
        return yaml.safe_load(stream)


class AzureConnector(object):
    def __init__(self, config):

        conf = (
            SparkConf()
            .set("spark.driver.maxResultSize", "5g")
            .set("spark.driver.bindAddress", "127.0.0.1")
        )

        self.session = SparkSession.builder.config(conf=conf).getOrCreate()
        self.session.conf.set(
            "fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net",
            config["fs.azure.account.auth.type"],
        )
        self.session.conf.set(
            "fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
            config["fs.azure.account.oauth.provider.type"],
        )
        self.session.conf.set(
            "fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
            config["fs.azure.account.oauth2.client.id"],
        )
        self.session.conf.set(
            "fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
            config["fs.azure.account.oauth2.client.secret"],
        )
        self.session.conf.set(
            "fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
            config["fs.azure.account.oauth2.client.endpoint"],
        )

    def get_session(self):
        return self.session


class AsureReader(object):
    def __init__(
        self,
        session: pyspark.sql.session.SparkSession,
        data_path: str,
        file_format: str,
    ):
        self.session = session
        self.data_path = data_path
        self.file_format = file_format

    def read(self):
        return (
            self.session.read.format(self.file_format)
            .options(header="true", inferschema="true")
            .load(self.data_path)
        )
