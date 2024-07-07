from .utils import preparing_environment, download_install_java, \
    download_install_elasticsearch, download_install_spark


def start():
    preparing_environment()
    download_install_java()
    download_install_elasticsearch()
    spark, sc = download_install_spark()
    return spark, sc