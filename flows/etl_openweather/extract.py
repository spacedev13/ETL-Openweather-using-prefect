import pandas as pd
from api import weather
from prefect import task, logging

@task(name='Extract / Transform weather')
def insert_weather():
    logger = logging.get_run_logger()
    logger.info('Iniciando extracao weather')
    retorno = weather()
    df = pd.DataFrame(retorno['weather'])
    df = df.astype(object)
    #df = df.itertuples(index=False, name=None)
    return list(df.itertuples(index=False, name=None))

@task(name='Extract / Transform coord')
def insert_coord():
    logger = logging.get_run_logger()
    logger.info('Iniciando extracao coord')
    retorno = weather()
    df = pd.DataFrame([retorno['coord']])
    df = df.astype(object)
    return list(df.itertuples(index=False, name=None))

@task(name='Extract / Transform main')
def insert_main():
    logger = logging.get_run_logger()
    logger.info('Iniciando extracao main')
    retorno = weather()
    df = pd.DataFrame([retorno['main']])
    df = df.astype(object)
    return list(df.itertuples(index=False, name=None))

@task(name='Extract / Transform sys')
def insert_sys():
    logger = logging.get_run_logger()
    logger.info('Iniciando extracao sys')
    retorno = weather()
    df = pd.DataFrame([retorno['sys']])
    df = df.astype(object)
    return list(df.itertuples(index=False, name=None))

@task(name='Extract / Transform wind')
def insert_wind():
    logger = logging.get_run_logger()
    logger.info('Iniciando extracao wind')
    retorno = weather()
    df = pd.DataFrame([retorno['wind']])
    df = df.astype(object)
    return list(df.itertuples(index=False, name=None))
