from setuptools import setup, find_packages

setup(
    name='linkageColabEnv',
    version='0.3',
    packages=['linkageColabEnv'],
    install_requires=[],
    url='https://github.com/joseaugustoduarte/linkageColabEnv',
    license='MIT License',
    author='José Augusto Duarte',
    author_email='joseaugustoduarte@gmail.com',
    description='Download, install and configure an environment to perform data engineering and data science on Google Colab, using Java, Spark and Elasticsearch.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown')