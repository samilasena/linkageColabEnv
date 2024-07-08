import os

def preparing_environment():
    CLEAR_DATA_COMMAND = f"rm -r /content/*"
    if os.system(CLEAR_DATA_COMMAND) != 0: print(f'Error: {CLEAR_DATA_COMMAND}')

    UPDATE_UPGRADE_COMMAND = f"apt-get update && apt-get upgrade"
    if os.system(UPDATE_UPGRADE_COMMAND) != 0: print(f'Error: {UPDATE_UPGRADE_COMMAND}')

    PIP_UPGRADE = f"pip3 install --upgrade requests"
    if os.system(PIP_UPGRADE) != 0: print(f'Error: {PIP_UPGRADE}')

    print('Limpeza do diretório Raiz, Atualizações do S.O e do PIP: -> OK')


def download_install_java():
    DOWNLOAD_INTALL_JAVA_COMMAND = f"apt-get install openjdk-11-jdk-headless -qq > /dev/null"
    if os.system(DOWNLOAD_INTALL_JAVA_COMMAND) != 0: print(f'Error: {DOWNLOAD_INTALL_JAVA_COMMAND}')

    os.environ["JAVA_HOME"] = f"/usr/lib/jvm/java-11-openjdk-amd64"

    print('Download, instalação e configuração do Java 11 -> OK')


def download_install_spark():
    SPARK_URL = "https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz"
    SPARK_VERSION = SPARK_URL.split('/')[-1]
    SPARK_PATH = f"/content/{SPARK_VERSION.replace('.tgz', '')}"
    WGET_COMMAND = f"wget -q {SPARK_URL}"
    TAR_COMMAND = f"tar xf {SPARK_VERSION}"

    if os.system(WGET_COMMAND) != 0: print(f'Error: {WGET_COMMAND}')
    if os.system(TAR_COMMAND) != 0: print(f'Error: {TAR_COMMAND}')

    FINDSPARK_COMMAND = f"pip install -q findspark"
    if os.system(FINDSPARK_COMMAND) != 0: print(f'Error: {FINDSPARK_COMMAND}')

    # Setting environment variables
    os.environ["SPARK_HOME"] = SPARK_PATH

    # Using findspark package to configure
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession
    from pyspark import SparkContext

    # Spark context creation
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("oficina_linkage_2024")\
        .config("spark.sql.parquet.compression.codec", "gzip")\
        .getOrCreate()
    sc = spark.sparkContext
    # spark.conf.set("write.parquet.compression-codec", "gzip")

    from pyspark.sql import functions as F
    from pyspark.sql import SQLContext
    from pyspark.sql import Window
    # from pyspark.sql.types import *

    print('Download, instalação e configuração do Spark 3.5.1 -> OK')

    os.system("rm -rf /content/*.tgz")

    return spark, sc


def download_install_elasticsearch():

    # obter a versão da lib python em: https://pypi.org/project/elasticsearch/
    # obter a versão do elastic build_flavor 'oss' em: https://www.elastic.co/pt/downloads/past-releases/elasticsearch-oss-7-10-2

    import time

    VERSION_ES = '7.10.2'
    VERSION_ES_PIP = '7.10.1'

    PIP_ES_COMMAND = f"pip install elasticsearch=={VERSION_ES_PIP}"
    if os.system(PIP_ES_COMMAND) != 0: print(f'Error: {PIP_ES_COMMAND}')

    ELASTIC_TAR_GZ_URL = f"https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-{VERSION_ES}-linux-x86_64.tar.gz"
    WGET_COMMAND_TAR_GZ = f"wget -q {ELASTIC_TAR_GZ_URL}"
    if os.system(WGET_COMMAND_TAR_GZ) != 0: print(f'Error: {WGET_COMMAND_TAR_GZ}')

    ELASTIC_TAR_GZ_SHA512_URL = f"https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-{VERSION_ES}-linux-x86_64.tar.gz.sha512"
    WGET_COMMAND_GZ_SHA512 = f"wget -q {ELASTIC_TAR_GZ_SHA512_URL}"
    if os.system(WGET_COMMAND_GZ_SHA512) != 0: print(f'Error: {WGET_COMMAND_GZ_SHA512}')

    TAR_COMMAND = f"tar -xzf {ELASTIC_TAR_GZ_URL.split('/')[-1]}"
    if os.system(TAR_COMMAND) != 0: print(f'Error: {TAR_COMMAND}')

    SUDO_COMMAND = f"sudo chown -R daemon:daemon {ELASTIC_TAR_GZ_URL.split('/')[-1].replace(f'-oss-{VERSION_ES}-linux-x86_64.tar.gz', f'-{VERSION_ES}')}"
    if os.system(SUDO_COMMAND) != 0: print(f'Error: {SUDO_COMMAND}')

    SHASUM_COMMAND = f"shasum -a 512 -c {ELASTIC_TAR_GZ_SHA512_URL.split('/')[-1]}"
    if os.system(SHASUM_COMMAND) != 0: print(f'Error: {SHASUM_COMMAND}')

    os.environ["ELASTICSEARCH_HOME"] = f"/content/elasticsearch-{VERSION_ES}/bin/elasticsearch"

    BG_ES_COMMAND = f"sudo -H -u daemon elasticsearch-{VERSION_ES}/bin/elasticsearch &"
    if os.system(BG_ES_COMMAND) != 0: print(f'Error: {BG_ES_COMMAND}')

    print('Download, instalação e configuração do Elasticsearch 7.10.2 -> OK')

    os.system("rm -rf /content/*.gz*")

    time.sleep(5)

    # print(os.popen('ps -ef | grep elasticsearch').read())

    # time.sleep(5)

    # print(os.popen('curl -sX GET "localhost:9200"').read())


def download_datasets():
    LIB = f"git clone https://github.com/joseaugustoduarte/linkageColabEnv.git"
    DATA = f"git clone https://github.com/joseaugustoduarte/linkage_database.git"

    if os.system(LIB) != 0: print(f'Error: {LIB}')
    if os.system(DATA) != 0: print(f'Error: {DATA}') 


def start():
  preparing_environment()
  download_install_java()
  download_install_elasticsearch()
  download_datasets()
  spark, sc = download_install_spark()
  return spark, sc