import asyncio
import base64
import gc
import json
import os
import re
import shutil
import subprocess
import time
from contextlib import contextmanager
from functools import wraps
from itertools import chain
from multiprocessing import set_start_method
from multiprocessing import cpu_count
from multiprocessing import Pool
from joblib import Parallel, delayed
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import asyncpg
import boto3
import numpy as np
import pandas as pd
import ast
from datetime import datetime
from boto3.resources.factory import ServiceResource
from botocore.exceptions import ClientError
from loggers import configure_logging
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, exceptions, helpers
from opensearchpy.client import OpenSearch as OpenSearchClient

from psycopg2 import sql
from psycopg2.errors import InternalError
from psycopg2.errors import InvalidSchemaName
from psycopg2.errors import OperationalError
from psycopg2.errors import ProgrammingError
from psycopg2.errors import UndefinedTable
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.types import BIGINT
from sqlalchemy.types import Boolean
from sqlalchemy.types import INT
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import TIMESTAMP

# Setting logger
logging = configure_logging()
_logger = logging.getLogger("generic-utils")


try:
    # Use multiprocessing-method "spawn"
    set_start_method("spawn")
except Exception as e:
    print(e)


@contextmanager
def timer(text: str):
    """
    This method wraps the given function and prints its time of execution accompanied by the provided text
    :param text: text to be included in the printouts
    :return: print execution time of the function along with the provided text
    """
    t0 = time.time()
    yield
    _logger.info("{} - done in {:.0f}s".format(text, time.time() - t0))


def timing(func: callable):
    """
    Wrapper of a function to show time of its execution
    :param func: func to decorate
    :return: print time of execution of a given function
    """

    @wraps(func)
    def wrap(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        _logger.info("'{0}' took {1:2.3f} sec".format(func.__name__, te - ts))
        return result

    return wrap


def parallelize(data, func: callable, n_data_chunks: int = -1, n_jobs: int = -1,
                concat_ignore_idx: bool = False, copy: bool = False):
    """
    This method applies any callable function to the input data using multiprocessing pool
    :param data: pd.DataFrame / pd.Series
    :param func: function to be mapped on data
    :param n_data_chunks: number of chunks to split data (e.g. number of parts one split pandas column). Normally, it
                          should be smaller than n_jobs
    :param n_jobs: number of threads to be used for running multiprocessing Pool
    :param concat_ignore_idx: if True -> ignore index when concatenating results into a single DF
    :param copy: if False -> do not copy data unnecessarily
    :return: processed data (e.g. pandas Series)
    """
    assert callable(func), "Argument func should be a callable function. Instead got %s" % type(func)
    assert isinstance(n_jobs, int), "Argument n_jobs of parallelize method should be int. " \
                                    "Instead provided %s" % type(n_jobs)

    assert isinstance(n_jobs, int), "Argument n_jobs of parallelize method should be int. " \
                                    "Instead provided %s" % type(n_jobs)

    n_data_chunks = cpu_count() if n_data_chunks == -1 else n_data_chunks  # number of CPU cores on your system
    n_jobs = cpu_count() if n_jobs == -1 else n_jobs  # number of CPU cores on your system

    _logger.info("Splitting input data into %d batches" % n_data_chunks)
    batches = np.array_split(data, n_data_chunks)

    _logger.info("Starting parallel processing using %d CPU" % n_jobs)
    pool = Pool(n_jobs)
    results = pool.map(func, batches)

    if isinstance(results[0], pd.DataFrame) or isinstance(results[0], pd.Series):
        _logger.info("Concatenating results into single DF")
        results = pd.concat(results, ignore_index=concat_ignore_idx, copy=copy)
    elif isinstance(results[0], List):
        _logger.info("Concatenating results into single list")
        results = list(chain(*results))
    else:
        pass

    pool.close()
    pool.join()

    del pool, batches
    gc.collect()

    return results


def parallelize_v2(data, func: callable, n_jobs: int = -1,
                   concat_ignore_idx: bool = False, copy: bool = False):
    """
    This method applies any callable function to the input data using multiprocessing pool
    :param data: pd.DataFrame / pd.Series
    :param func: function to be mapped on data
    :param n_jobs: number of threads to be used for running multiprocessing Pool
    :param concat_ignore_idx: if True -> ignore index when concatenating results into a single DF
    :param copy: if False -> do not copy data unnecessarily
    :return: processed data (e.g. pandas Series)
    """

    # TODO: this does not work properly !!!!!!!!!!!!!

    assert callable(func), "Argument func should be a callable function. Instead got %s" % type(func)
    assert isinstance(n_jobs, int), "Argument n_jobs of parallelize method should be int. " \
                                    "Instead provided %s" % type(n_jobs)

    assert isinstance(n_jobs, int), "Argument n_jobs of parallelize method should be int. " \
                                    "Instead provided %s" % type(n_jobs)

    _logger.info("Starting parallel processing using %d CPU" % n_jobs)
    with Parallel(n_jobs=n_jobs, prefer="processes") as parallel:
        results = parallel(delayed(func)(entry) for entry in data)

    if isinstance(results[0], pd.DataFrame) or isinstance(results[0], pd.Series):
        _logger.info("Concatenating results into single DF")
        results = pd.concat(results, ignore_index=concat_ignore_idx, copy=copy)
    elif isinstance(results[0], List):
        _logger.info("Concatenating results into single list")
        results = list(chain(*results))
    else:
        pass

    return results


def downcast_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimizes data-types in a pandas DF to reduce memory allocation
    :param df: input pandas DF
    :return: pandas DF with optimized data-types
    """
    start_mem = df.memory_usage().sum() / 1024 ** 2
    _logger.info('Memory usage of data frame is {:.2f} MB'.format(start_mem))

    for col in df.columns:
        col_type = df[col].dtype
        if col_type != object and col_type != 'datetime64[ns]':
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    end_mem = df.memory_usage().sum() / 1024 ** 2
    _logger.info('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    _logger.info('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    return df


def create_output_dir(path_output_dir: str, silent: bool = False) -> str:
    """
    This method creates directory if it not existed at the moment of request
    :param path_output_dir: absolute path to a file / directory
    :param silent: if True -> do not print the message that directory is created
    :return: path_output_dir string
    """
    if not os.path.exists(path_output_dir):
        os.makedirs(path_output_dir)
        if not silent:
            _logger.info(f"Output directory '{path_output_dir}' is created")
    return path_output_dir


def delete_output_dir(path_output_dir: str, silent: bool = False) -> None:
    """
    This method deletes directory and all files in it (if it exists)
    :param path_output_dir: absolute path to a file / directory
    :param silent: if True -> do not print the message that directory is created
    :return: None
    """
    if os.path.exists(path_output_dir):
        shutil.rmtree(path_output_dir)
        if not silent:
            _logger.info(f"Output directory '{path_output_dir}' (and all files in it, if any) is deleted")


def delete_local_file(path_to_file: str, filename: str):
    """
    This method deletes file at given location
    :param path_to_file:
    :param filename:
    :return:
    """

    if os.path.exists(os.path.join(path_to_file, filename)):
        _logger.info(f"Deleting '{os.path.join(path_to_file, filename)}'")
        os.remove(os.path.join(path_to_file, filename))
        _logger.debug("File deleted")
    else:
        _logger.error(f"File {os.path.join(path_to_file, filename)} does not exists")


def subprocess_cmd(commands: str) -> Tuple:
    """
    This method is used to run shell commands in python
    :param commands: string command to run in shell (e.g. ls -lh)
    :return: (stdout, error)
    """
    process = subprocess.Popen(commands, stdout=subprocess.PIPE, shell=True)  # nosec
    stdout, error = process.communicate()
    _logger.debug(f"Stdout: {stdout}. Error: {error}")
    return stdout, error


def get_postgres_engine(config: dict) -> Engine:
    """
    This method returns sql alchemy engine. One can use it to execute queries against DB.
    :param config: dict with settings required to establish connection with the database (username, password, host,
                   port and database name
    :return: sql alchemy engine
    """
    username = config['username']
    password = config['password']
    host = config['host']
    port = config['port']
    database = config['dbname']
    sa_engine = create_engine('postgresql+psycopg2://{u}:{pa}@{h}:{po}/{db}'.format(u=username, pa=password,
                                                                                    h=host, po=port,
                                                                                    db=database))
    return sa_engine


def get_asyncpg_conn(config: dict):
    """
    This method returns asyncpg connection for postgres (https://github.com/MagicStack/asyncpg).
    It does not work with AWS postgres
    :return: asyncpg connection
    """
    username = config['username']
    password = config['password']
    host = config['host']
    port = config['port']
    database = config['dbname']
    conn = asyncpg.connect(user=username, password=password, database=database, host=host, port=port)
    return conn


def get_asyncpg_pool(config: dict, max_size: int = 100, max_queries: int = 500000):
    """
    This method returns asyncpg pool (https://github.com/MagicStack/asyncpg).
    :return: asyncpg pool
    """
    username = config['username']
    password = config['password']
    host = config['host']
    port = config['port']
    database = config['dbname']

    pool = asyncpg.create_pool(user=username, password=password, database=database, host=host, port=port,
                               max_size=max_size, max_queries=max_queries)
    return pool


def prettify_query_outlook(query: str):
    """
    This method improves query readability (especially for logging purposes)
    :param query: input query
    :return: cleaned input query
    """
    if query:
        query = re.sub(r'[^\S\n]+', ' ', query)
        query = re.sub('\n ', '\n', query)
        query = re.sub('\n\n', '\n', query)
    return query


def execute_query(sql_engine: Engine, query: str, print_response: bool = True) -> Union[List, None]:
    """
    This method executes query using DB connection object. It is potentially vulnerable for SQL injection.
    :param sql_engine: postgres or postgres SQL engine (use get_postgres_engine() or get_postgres_engine() methods)
    :param query: single query to be executed
    :param print_response: if True -> will only print response to the query (not return),
                           if False -> will return the response
    :return:
    """

    _logger.info(prettify_query_outlook(query))

    try:
        with sql_engine.begin() as conn:
            result = conn.execute(query)

            # If there  is anything to print
            if len(result.keys()):
                if print_response:
                    # Just print response
                    for k in result:
                        _logger.info(k)
                else:
                    # Return response
                    return result.fetchall()

    except InternalError as e:
        # This exception captures errors for postgres that requires check of stl_load_errors
        _logger.error(e)
        raise

    except Exception as e:
        # Some errors occurred due to very weird behavior of DB. Majority I had where caused by backend
        # after query has been successfully performed -> no need to terminate the flow because of these
        # errors. I don't like this way of handling the problem - but it's ok as temporary solution!
        _logger.error(e)
        return

    finally:
        condition = conn.closed == 0

        if conn and condition:
            conn.close()
            del conn


def execute_queries(sql_engine: Engine, list_queries: List, print_response: bool = False,
                    time_sleep: int = 1) -> Union[List[List], None]:
    """
    This method is designed to execute sequentially a list of queries. It also fixes the problem of connection being
    closed after creating materialized view in postgres DB.
    :param sql_engine: postgres or postgres SQL engine (use get_postgres_engine() or get_postgres_engine() methods)
    :param list_queries: list of queries
    :param print_response: if True -> will only print response to the query (not return),
                           if False -> will return the response
    :param time_sleep: sleep time (in seconds) between two sequential queries. It is needed (sometimes) to overcome
                       OperationalError: SSL SYSCALL error: EOF detected (it happens when 1 query was running for a
                       long time and results were stored to a table, and the next query is accessing that table).
    :return:
    """

    assert len(list_queries), "There are no queries in list_queries"

    result = []
    for i, query in enumerate(list_queries):
        _logger.debug("Executing #%d query" % i)
        try:
            result.append(execute_query(sql_engine=sql_engine, query=query, print_response=print_response))
        except ResourceClosedError:
            # I have no clue why the connection is closed when a materialized view is created
            continue
        time.sleep(time_sleep)
    return result if not print_response else None


def execute_query_safely(sql_engine: Engine, query: Union[sql.SQL, sql.Composed]) -> Union[List, None]:
    """
    This method executes query in safe manner using DB cursor object. It is safe against SQL injection.
    :param sql_engine: postgres or postgres SQL engine (use get_postgres_engine() or get_postgres_engine() methods)
    :param query: single query to be executed
    :return:
    """

    conn = sql_engine.raw_connection()

    try:
        with conn.cursor() as cursor:
            query_log = prettify_query_outlook(query.as_string(cursor))
            _logger.info(query_log)
            cursor.execute(query)

            try:
                result = cursor.fetchall()
                if result and len(result) > 0:
                    return result
                return

            except ProgrammingError as e:
                # This type of exception occurs when we are uploading data
                # It's not an error!!
                if str(e).strip().lower() == 'no results to fetch':
                    conn.commit()
                    return
                else:
                    raise

            except OperationalError as e:
                _logger.debug("Conn closed: ", conn.closed)
                _logger.debug("Cursor closed: ", cursor.closed)

                if str(e).strip() == 'SSL SYSCALL error: EOF detected':
                    _logger.warn(e)
                    # conn.rollback()
                    return

    except InternalError as e:
        # This exception captures errors for postgres that requires check of stl_load_errors
        _logger.error(e)
        raise

    except UndefinedTable as e:
        # This exception captures errors when we use check_table_exists_and_not_empty() method
        _logger.warn(e)
        raise

    except Exception as e:
        # Some errors occurred due to very weird behavior of DB. Majority I had where caused by backend
        # after query has been successfully performed -> no need to terminate the flow because of these
        # errors. I don't like this way of handling the problem - but it's ok as temporary solution!
        _logger.error(e)
        # conn.rollback()
        return

    finally:
        if cursor and not cursor.closed:
            cursor.close()

        if conn and conn.closed == 0:
            conn.close()
            del conn


async def asyncpg_run_query(conn, query: str):
    """
    This method is used to run single query with asyncpg
    :param conn: asyncpg connection
    :param query: single query to be executed
    :return:
    """
    conn_async = await conn
    values = await conn_async.fetch(query)
    await conn_async.close()
    return values


async def asyncpg_run_in_pool_single_query(pool, query: str, pool_timeout: int = 7200):
    """
    This method is used to run single query in asyncpg connection pool
    :param pool: asyncpg pool
    :param query: single query to be executed
    :param pool_timeout: timeout in seconds
    :return:
    """
    await pool
    con = await pool.acquire(timeout=pool_timeout)

    try:
        values = await con.fetch(query)
    finally:
        await pool.release(con)
    return values


async def asyncpg_run_in_pool_multiple_queries(pool, queries: List, pool_timeout: int = 7200):
    """
    This method is used to run list of queries in the asyncpg connection pool
    :param pool: asyncpg pool
    :param queries: list of queries (string)
    :param pool_timeout: timeout in sec used by asyncpg pool
    :return:
    """
    await pool

    async def worker(query):
        con = await pool.acquire(timeout=pool_timeout)
        values_ = await con.fetch(query)
        return values_

    tasks = [worker(query) for query in queries]
    values = await asyncio.gather(*tasks)
    return list(chain.from_iterable(values))


async def asyncpg_copy_records_to_table(conn, records: List[Tuple], table_name: str,
                                        schema_name: Union[str, None] = None):
    """
    This is very fast method of loading data into database table. Before using this method, one should create a
    table that match records structure.
    :param conn: asyncpg connection
    :param records: list of tuples to be loaded to database
    :param table_name: name of table in the database
    :param schema_name: name of schema in the database
    :return:
    """

    conn_async = await conn
    result = await conn_async.copy_records_to_table(table_name=table_name, records=records, schema_name=schema_name)
    _logger.info(result)
    await conn_async.close()
    _logger.debug(f"Is asyncpg connection closed: {conn_async.is_closed()}")


def get_secret_from_aws_secrets_manager(secret_name: str, region_name: str = "us-east-1") -> Union[Dict, None]:
    """
    This is generic method that fetches secret from AWS Secrets Manager
    :param secret_name: name of secret
    :param region_name: name of aws region
    :return:
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            # Catch-all exception
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        if secret:
            return json.loads(secret)
        else:
            _logger.error("Secret '%s' is None" % secret_name)
            return
    return


def check_table_exists_and_not_empty(table_name: str, schema: str, sql_engine: Engine) -> bool:
    """
    This method checks whether table exists in the given schema
    :param table_name: name of table in postgres
    :param schema: postgres schema where the table is located
    :param sql_engine: SQL engine (postgres, postgres, etc...)
    :return:
    """

    query = sql.SQL("SELECT * FROM {schema}.{table_name} LIMIT 1").format(schema=sql.Identifier(schema),
                                                                          table_name=sql.Identifier(table_name))

    try:
        response = execute_query_safely(sql_engine=sql_engine, query=query)
        return True if response and len(response) > 0 else False
    except UndefinedTable:
        return False
    except InvalidSchemaName:
        return False
    except Exception as e:
        _logger.warn(e)
        return False


def normalize_path_output_dir_s3(path_output_dir_s3: str) -> str:
    """
    This method is removing s3:// or S3:// from provided path to output directory on S3
    :param path_output_dir_s3: path to output directory on S3
    :return:
    """
    return path_output_dir_s3.replace("s3://", "").replace("S3://", "")


def get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3: str) -> Tuple[str, str]:
    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3)
    bucket_name = path_output_dir_s3.split('/')[0]
    s3_prefix = '/'.join(path_output_dir_s3.split('/')[1:])
    return bucket_name, s3_prefix


def s3_bucket_exists(s3_resource: ServiceResource, bucket: str) -> bool:
    bucket_exists = False
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket)
        bucket_exists = True
    except ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = e.response['Error']['Code']
        if error_code == '403':
            _logger.debug("Private Bucket. Forbidden Access!")
            bucket_exists = True
        elif error_code == '404':
            _logger.debug("Bucket Does Not Exist!")
            bucket_exists = False
    return bucket_exists


def create_s3_bucket(s3_resource: ServiceResource, bucket: str, region: str = "us-east-1") -> None:
    """
    This method creates S3 bucket
    :param s3_resource: s3 ServiceResources
    :param bucket: name of bucket on s3
    :param region: AWS region (default is 'us-east-1')
    :return:
    """
    if not s3_bucket_exists(s3_resource, bucket=bucket):
        _logger.info("Creating empty bucket: 's3://{bucket}' (region: '{region}')".format(bucket=bucket, region=region))

        # If region is 'us-east-1' -> don't specify an s3 region: https://github.com/boto/boto3/issues/125
        # Bug fix here: https://github.com/artefactual/archivematica-storage-service/pull/488
        if region == 'us-east-1':
            s3_resource.create_bucket(ACL="private", Bucket=bucket)
        else:
            s3_resource.create_bucket(ACL="private", Bucket=bucket,
                                      CreateBucketConfiguration={'LocationConstraint': region})

        # Wait for bucket creation
        attempts = 3

        while attempts > 0:
            bucket_exists = s3_bucket_exists(s3_resource=s3_resource, bucket=bucket)
            if bucket_exists:
                break
            else:
                time.sleep(2)
                attempts -= 1

        assert s3_bucket_exists(s3_resource=s3_resource, bucket=bucket), f"Bucket {bucket} was not created"

        # Applying block on public access
        s3_resource.meta.client.put_public_access_block(Bucket=bucket,
                                                        PublicAccessBlockConfiguration={
                                                            'BlockPublicAcls': True,
                                                            'IgnorePublicAcls': True,
                                                            'BlockPublicPolicy': True,
                                                            'RestrictPublicBuckets': True
                                                        })
        _logger.debug("Bucket created")
    else:
        _logger.warn(f"Bucket: 's3://{bucket}' already exists ...")


def get_s3_bucket_object(s3_resource: ServiceResource, bucket: str):
    """
    This method returns s3 bucket object if that exists
    :param s3_resource: s3 ServiceResources
    :param bucket: name of bucket on s3
    :return:
    """
    if s3_bucket_exists(s3_resource, bucket=bucket):
        return s3_resource.Bucket(bucket)
    else:
        _logger.error("Bucket '%s' does not exists ..." % bucket)
    return


def list_file_objs_in_s3_dir(s3_resource: ServiceResource, path_output_dir_s3: str, include_dir_name: bool = False):

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_obj = get_s3_bucket_object(s3_resource=s3_resource, bucket=bucket_name)

    if bucket_obj:
        objects = [obj for obj in bucket_obj.objects.filter(Prefix=s3_prefix)]

        if s3_prefix:
            if objects and not include_dir_name:
                return [obj for obj in objects if obj.key != s3_prefix]
            return objects
        else:
            return objects
    return []


def check_file_obj_exists_in_s3_dir(s3_resource: ServiceResource, path_output_dir_s3: str, filename: str):

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    objects = list_file_objs_in_s3_dir(s3_resource=s3_resource, path_output_dir_s3=path_output_dir_s3)

    if objects:
        match = [obj.key for obj in objects if os.path.normpath(obj.key) == os.path.normpath(
            os.path.join(s3_prefix, filename))]
        if match:
            _logger.info(f"File '{filename}' is in 's3://{bucket_name}/{s3_prefix}'")
            return True
        _logger.warn(f"File '{filename}' is not in 's3://{bucket_name}/{s3_prefix}'")
    return


def rename_file_object_s3(s3_resource: ServiceResource, path_output_dir_s3: str, original_fn: str, new_fn: str):

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)

    if check_file_obj_exists_in_s3_dir(s3_resource=s3_resource, path_output_dir_s3=path_output_dir_s3,
                                       filename=original_fn):
        orig_prefix_fn = os.path.join(s3_prefix, original_fn).replace('\\', '/')
        new_prefix_fn = os.path.join(s3_prefix, new_fn).replace('\\', '/')
        _logger.info(f"Renaming '{orig_prefix_fn}' to '{new_prefix_fn}'")

        s3_resource.Object(bucket_name, new_prefix_fn).copy_from(CopySource=os.path.join(path_output_dir_s3,
                                                                                         original_fn),
                                                                 ACL='private')
        s3_resource.Object(bucket_name, orig_prefix_fn).delete()


def delete_s3_bucket(s3_resource: ServiceResource, bucket: str) -> None:
    """
    This method deletes s3 bucket and all files inside.

    BE VERY CAREFUL WHEN USING IT !!!

    :param s3_resource: s3 ServiceResources
    :param bucket: name of bucket on s3
    :return:
    """
    bucket_obj = get_s3_bucket_object(s3_resource, bucket)

    if bucket_obj:
        _logger.info(f"Deleting 's3://{bucket}' and its content")

        for key in bucket_obj.objects.all():
            key.delete()
        bucket_obj.delete()

        _logger.debug("Bucket deleted")
    else:
        _logger.error(f"Bucket '{bucket}' does not exists. Nothing to delete ...")


def delete_s3_dir(s3_resource: ServiceResource, path_output_dir_s3: str) -> None:
    """
    This method deletes s3 directory (i.e. bucket_name + prefix (if any)) and all files inside.
    It does not delete the bucket itself - for that use delete_s3_bucket() method

    BE VERY CAREFUL WHEN USING IT !!!

    :param s3_resource: s3 ServiceResources
    :param path_output_dir_s3: name of bucket on s3
    :return:
    """

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    objects = list_file_objs_in_s3_dir(s3_resource=s3_resource, path_output_dir_s3=path_output_dir_s3,
                                       include_dir_name=True)

    if objects:
        _logger.info(f"Deleting 's3://{path_output_dir_s3}' directory and its content")

        for key in objects:
            key.delete()

        _logger.debug("Deleted")


def delete_file_object_s3(s3_resource: ServiceResource, path_output_dir_s3: str, filename: str) -> None:

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    s3_prefix_plus_fn = os.path.join(s3_prefix, filename).replace('\\', '/')

    if check_file_obj_exists_in_s3_dir(s3_resource=s3_resource, path_output_dir_s3=path_output_dir_s3,
                                       filename=filename):
        _logger.info(f"Deleting 's3://{s3_prefix_plus_fn}'")
        s3_resource.Object(bucket_name, s3_prefix_plus_fn).delete()
        _logger.debug("Deleted")


def copy_file_to_s3(s3_resource: ServiceResource, path_local_dir: str, filename: str, path_output_dir_s3: str):

    assert filename, "Please provide name of the file to be copied from local machine to s3"
    assert path_local_dir, "Please provide path to the local directory with the file"
    assert path_output_dir_s3, "Please provide path to the directory on s3 where file to be copied"


    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)

    assert os.path.isdir(path_local_dir), f"The 'path_local_dir' is the full path to the directory with " \
                                          f"the file that should be copied to s3. Received path '{path_local_dir}' " \
                                          f"either does not exist or is not a directory "

    assert os.path.exists(os.path.join(path_local_dir, filename)), "File '%s' does not exist" % os.path.join(
        path_local_dir, filename)

    if not s3_bucket_exists(s3_resource=s3_resource, bucket=bucket_name):
        create_s3_bucket(s3_resource=s3_resource, bucket=bucket_name)

    _logger.info(f"Uploading '{filename}' from '{path_local_dir}' to 's3://{bucket_name}/{s3_prefix}'")

    s3_resource.meta.client.upload_file(Filename=os.path.join(path_local_dir, filename),
                                        Bucket=bucket_name,
                                        Key=os.path.join(s3_prefix, filename).replace('\\', '/'))
    _logger.debug("File uploaded")


def download_file_from_s3(s3_resource: ServiceResource, path_output_dir_s3: str, filename: str, path_local_dir: str):


    assert filename, "Please provide name of the file to be copied from s3 to local machine"
    assert path_output_dir_s3, "Please provide path to the directory on s3 containing file to be downloaded"
    assert path_local_dir, "Please provide path to the local directory were to save the file from S3"

    path_output_dir_s3 = normalize_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)
    bucket_name, s3_prefix = get_bucket_name_and_prefix_from_path_output_dir_s3(path_output_dir_s3=path_output_dir_s3)

    assert check_file_obj_exists_in_s3_dir(s3_resource=s3_resource, path_output_dir_s3=path_output_dir_s3,
                                           filename=filename), \
        f"File '{filename}' is not in 's3://{bucket_name}/{s3_prefix}'"
    create_output_dir(path_local_dir)

    _logger.info(f"Downloading '{filename}' from 's3://{bucket_name}/{s3_prefix}' to '{path_local_dir}'")

    s3_resource.meta.client.download_file(Bucket=bucket_name,
                                          Key=os.path.normpath(os.path.join(s3_prefix, filename)).replace('\\', '/'),
                                          Filename=os.path.normpath(os.path.join(path_local_dir, filename)))
    _logger.debug("File downloaded")


def list_executions_by_status(s3_step_func_client, state_machine_arn: str, execution_status: str = None) -> List:
    """
    This method is used to list all step functions executions for selected state machine
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :param execution_status: filter step functions by execution status (one of 'RUNNING', 'SUCCEEDED', 'FAILED',
                            'TIMED_OUT', 'ABORTED')
    :return:
    """

    assert state_machine_arn, f"Please provide state_machine_arn. Received {state_machine_arn}"
    assert execution_status in ['RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED', None], \
        f"Execution status filter should be one of the following: ['RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', " \
        f"'ABORTED', None]. Instead got: {execution_status}"

    kwargs = dict()
    kwargs['stateMachineArn'] = state_machine_arn
    
    if execution_status:
        kwargs['statusFilter'] = execution_status
    
    response = s3_step_func_client.list_executions(**kwargs)
    do_continue = response.get('nextToken')

    result = []
    if 'executions' in response:
        result.extend(response['executions'])
    while do_continue:
        response = s3_step_func_client.list_executions(**kwargs, nextToken=do_continue)
        do_continue = response.get('nextToken')
        if 'executions' in response:
            result.extend(response['executions'])
    return result 


def get_successful_ingestions(s3_step_func_client, state_machine_arn: str) -> List:
    """
    This method returns a list of step functions with execution_status = 'SUCCEEDED'
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :return:
    """
    successful_ingestions = list_executions_by_status(s3_step_func_client=s3_step_func_client,
                                                      state_machine_arn=state_machine_arn, 
                                                      execution_status='SUCCEEDED')
    return successful_ingestions


def get_active_ingestions(s3_step_func_client, state_machine_arn: str) -> List:
    """
    This method returns a list of step functions with execution_status = 'RUNNING'
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :return:
    """
    active_ingestions = list_executions_by_status(s3_step_func_client=s3_step_func_client,
                                                  state_machine_arn=state_machine_arn, 
                                                  execution_status='RUNNING')
    return active_ingestions


def get_failed_ingestions(s3_step_func_client, state_machine_arn: str) -> List:
    """
    This method returns a list of step functions with execution_status = 'FAILED'
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :return:
    """
    failed_ingestions = list_executions_by_status(s3_step_func_client=s3_step_func_client,
                                                  state_machine_arn=state_machine_arn, 
                                                  execution_status='FAILED')
    return failed_ingestions


def get_aborted_ingestions(s3_step_func_client, state_machine_arn: str) -> List:
    """
    This method returns a list of step functions with execution_status = 'ABORTED'
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :return:
    """
    aborted_ingestions = list_executions_by_status(s3_step_func_client=s3_step_func_client,
                                                   state_machine_arn=state_machine_arn, 
                                                   execution_status='ABORTED')
    return aborted_ingestions


def get_timedout_ingestions(s3_step_func_client, state_machine_arn: str) -> List:
    """
    This method returns a list of step functions with execution_status = 'TIMED_OUT'
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param state_machine_arn: Amazon Resource Name for State Machine
    :return:
    """
    timed_out_ingestions = list_executions_by_status(s3_step_func_client=s3_step_func_client,
                                                     state_machine_arn=state_machine_arn,
                                                     execution_status='TIMED_OUT')
    return timed_out_ingestions


def get_execution_history_of_ingestion_run(s3_step_func_client, execution_arn: str, maxResults: int = None) -> List:
    """

    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param execution_arn: Amazon Resource Name for particular execution of the State Machine (i.e. ingestion run)
    :param maxResults: max number of results to return in 1 page (response of get_execution_history() is paginator)
    :return:
    """
    
    assert execution_arn, "Please provide execution_arn"

    kwargs = dict()
    kwargs['executionArn'] = execution_arn
    
    if maxResults:
        kwargs['maxResults'] = maxResults
    
    response = s3_step_func_client.get_execution_history(**kwargs)
    do_continue = response.get('nextToken')

    result = []
    if 'events' in response:
        result.extend(response['events'])
    while do_continue:
        response = s3_step_func_client.get_execution_history(**kwargs, nextToken=do_continue)
        do_continue = response.get('nextToken')
        if 'events' in response:
            result.extend(response['events'])
    return result 


def get_source_name_from_ingestion_run(s3_step_func_client, execution_arn: str) -> str:
    """
    This method returns the name of source(s) to be ingested in selected execution (defined by execution_arn)
    :param s3_step_func_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                                (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param execution_arn: Amazon Resource Name for particular execution of the State Machine (i.e. ingestion run)
    :return:
    """

    response = s3_step_func_client.describe_execution(executionArn=execution_arn)
    
    if response:
        source_name = ast.literal_eval(response['input'])['targets']
        return source_name
    return ''


def list_eventbridge_rules(s3_eventbridge_client, event_bus_name: str = 'default', name_prefix: str = None) -> List:
    """
    This method returns the list of events from selected Event Bus in Amazon EventBridge
    :param s3_eventbridge_client: boto3 low-level client representing Amazon EventBridge
                                  (created as s3_eventbridge_client = boto3.client('events'))
    :param event_bus_name: name of EventBridge Bus
    :param name_prefix: filter all events by the prefix (i.e. show only the events starting with the name_prefix)
    :return:
    """
    
    assert event_bus_name, "Please provide event_bus_name"
    
    kwargs = dict()
    kwargs['EventBusName'] = event_bus_name
    
    if name_prefix:
        kwargs['NamePrefix'] = name_prefix
    
    response = s3_eventbridge_client.list_rules(**kwargs)
    do_continue = response.get('nextToken')

    result = []

    if 'Rules' in response:
        result.extend(response['Rules'])
    while do_continue:
        response = s3_eventbridge_client.list_rules(**kwargs, nextToken=do_continue)
        do_continue = response.get('nextToken')
        if 'Rules' in response:
            result.extend(response['Rules'])
            
    return result 


def get_targets_for_eventbridge_rules(s3_eventbridge_client, eventbridge_rule_names: List[str], 
                                      event_bus_name: str) -> pd.DataFrame: 
    """
    This method returns the list of targets for each EventBridge rule provided in `eventbridge_rule_names`
    :param s3_eventbridge_client: boto3 low-level client representing Amazon EventBridge
                                  (created as s3_eventbridge_client = boto3.client('events'))
    :param eventbridge_rule_names: list of EventBridge rule names (for which to extract targets)
    :param event_bus_name: name of EventBridge Bus
    :return:
    """
    targets_in_rules_eb = []
    for rule_eb in eventbridge_rule_names:
        response = s3_eventbridge_client.list_targets_by_rule(
            Rule=rule_eb,
            EventBusName=event_bus_name,
        )
        
        try:
            targets_in_rule = []
            for t in response['Targets']:
                targets = ast.literal_eval(t['Input'])['body']['targets']
                targets_in_rule.append(targets)
            targets_in_rule = list(chain(*targets_in_rule))
        except Exception as e:
            _logger.info(f"There is no target for EB rule '{rule_eb}'")
            targets_in_rule = []
        
        targets_in_rules_eb.append((rule_eb, targets_in_rule))

    targets_in_rules_eb = pd.DataFrame(targets_in_rules_eb, columns=['event_rule_name_eb', 'source'])
    targets_in_rules_eb = targets_in_rules_eb.explode('source')
    return targets_in_rules_eb


def get_all_errors_from_log_group(s3_logs_client, log_group: str, logs_start_time: datetime,
                                  logs_end_time: datetime, limit: int = 10000) -> pd.DataFrame:
    """
    This method returns all logs in selected log_group that containing type = "Error". One can provide start_time and
    the end_time to filter logs by time. This method was mainly written to cover /ecs/dbtask log group.

    :param s3_logs_client: boto3 client to use for creating, managing, and running the workflow on Step Functions
                           (created as s3_step_func_client = boto3.client('stepfunctions'))
    :param log_group: name of log group in Amazon CloudWatch
    :param logs_start_time: start time of logs (to filter by time).
                            One can use, for instance:
                                logs_start_time = datetime.utcnow() - timedelta(days=15)  or
                                logs_start_time = datetime.fromisoformat("2021-11-01 00:00:00")
    :param logs_end_time: end time of logs (to filter by time)
                          One can use, for instance:
                                logs_end_time = datetime.utcnow()
                                logs_end_time = datetime.fromisoformat("2022-02-08 00:00:00")
    :param limit: max number of logs in 1 page
    :return:
    """

    _logger.info(f"Fetching logs for {log_group} log_group over period {logs_start_time.strftime('%Y-%m-%d %H:%M:%S')} "
                 f"-> {logs_end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Amazon CloudWatch insights query
    query = """
    fields @log, @logStream, @timestamp, type, guid, stack, target, task, msg, time, @message
    | filter level = 50
    """

    start_query_response = s3_logs_client.start_query(
        logGroupName=log_group,
        startTime=int(logs_start_time.timestamp()),
        endTime=int(logs_end_time.timestamp()),
        queryString=query,
        limit=limit
    )
    query_id = start_query_response['queryId']

    response = None
    while response is None or response['status'] == 'Running':
        _logger.info('Waiting for query to complete ...')
        time.sleep(1)
        response = s3_logs_client.get_query_results(queryId=query_id)
    _logger.info(f">>> Response stats: {response['statistics']}")

    _logger.info(f"There are {len(response['results'])} ingestions with errors found in the period "
                 f"{logs_start_time.strftime('%Y-%m-%d %H:%M:%S')} -> {logs_end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    logs = []
    for r in response['results']:
        logs.append({elem['field']: elem['value'] for elem in r})

    logs_df = pd.DataFrame(logs)
    logs_df.columns = logs_df.columns.str.strip('@')
    return logs_df


def map_pandas_dtypes_to_redshift_sql_dtypes(df: pd.DataFrame) -> Dict:
    """
    This method extracts and map pandas dtypes to SQL
    :param df: input pandas DD
    :return: dict with SQL dtypes
    """
    map_pandas_dtypes_to_sql = {
        'int64': BIGINT,
        'int32': INT,
        'float64': NUMERIC,
        'object': VARCHAR(length=65535),
        'str': VARCHAR(length=65535),
        'bool': Boolean,
        'datetime64[ns]': TIMESTAMP
    }

    table_structure = df.dtypes.astype(str).to_dict()
    table_structure = {k: map_pandas_dtypes_to_sql[v] for k, v in table_structure.items()}
    return table_structure


def map_pandas_dtypes_to_postgres_sql_dtypes(df: pd.DataFrame) -> Dict:
    """
    This method extracts and map pandas dtypes to SQL
    :param df: input pandas DD
    :return: dict with SQL dtypes
    """
    map_pandas_dtypes_to_sql = {
        'int64': 'bigint',
        'int32': 'integer',
        'int16': 'smallint',
        'float64': 'real',
        'float32': 'double precision',
        'float16': 'double precision',
        'object': 'varchar(65535)',
        'category': 'varchar(65535)',
        'str': 'varchar(65535)',
        'bool': 'boolean',
        'datetime64[ns]': 'timestamp'
    }
    table_structure = df.dtypes.astype(str).to_dict()
    table_structure = {k: map_pandas_dtypes_to_sql[v] for k, v in table_structure.items()}
    return table_structure


def dump_pgs_table_to_csv(table_name: str, pgs_schema: str, postgres_engine: Engine, path_to_file: str,
                          filename: str, delimiter: str = "|", null: str = ""):
    """
    This method is used to dump table from Postgres DB to csv file on EC2
    :param table_name: name of table in postgres
    :param pgs_schema: postgres schema where the table is located
    :param postgres_engine: postgres SQL engine
    :param path_to_file: full path to local directory on EC2 where dumped postgres table will be saved
    :param filename: name of csv file where dumped postgres table will be saved
    :param delimiter: delimiter to be used when saving postgres table to csv
    :param null: string that defines how the null should be treated
    :return:
    """

    if not os.path.exists(os.path.join(path_to_file, filename)):
        conn = postgres_engine.raw_connection()
        cursor = conn.cursor()
        _logger.info("Dumping '{pgs_schema}.{table_name}' table from postgres to '{file}'".format(
            pgs_schema=pgs_schema, table_name=table_name, file=os.path.join(path_to_file, filename)))

        with open(os.path.join(path_to_file, filename), 'w') as io:
            query = sql.SQL(
                """
                COPY (SELECT * FROM {pgs_schema}.{table_name}) TO STDOUT
                WITH DELIMITER '{delimiter}' NULL AS '{null}';
                """
            ).format(
                pgs_schema=sql.Identifier(pgs_schema),
                table_name=sql.Identifier(table_name),
                delimiter=sql.SQL(delimiter),
                null=sql.SQL(null)
            )
            cursor.copy_expert(query, io)
        conn.close()
        _logger.debug("File dumped")
    else:
        _logger.warn("File '{file}' already exists".format(file=os.path.join(path_to_file, filename)))


def _preserve_cols_order_in_fetched_scheme(fetched_table_scheme: List, table_name: str, db_schema: str,
                                           sql_engine: Engine) -> List:
    """
    This method gurantee that columns order in fetch_table_scheme() is actually match the order of columns in the
    source table. It may happen that fetch_table_scheme() does not preserve order - thus we need to have an
    additional check and alignment
    :param table_name: name of source table in DB
    :param db_schema: DB schema where the source table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    q2 = sql.SQL("SELECT * FROM {db_schema}.{table_name} LIMIT 0").format(db_schema=sql.Identifier(db_schema),
                                                                          table_name=sql.Identifier(table_name))
    conn = sql_engine.raw_connection()
    with conn.cursor() as cursor:
        q2 = q2.as_string(cursor)
    del conn

    # Source table columns order
    source_table_cols_order = pd.read_sql(con=sql_engine, sql=q2).columns.tolist()

    # Preserve same order of columns in fetched scheme as it is in the table itself
    table_scheme_cols_order_preserved = []
    for c in source_table_cols_order:
        for c1 in fetched_table_scheme:
            if c1[0] == c:
                table_scheme_cols_order_preserved.append(c1)
                break

    return table_scheme_cols_order_preserved


def fetch_table_scheme(table_name: str, db_schema: str, sql_engine: Engine) -> Union[List, None]:
    """
    This method reads scheme of table in postgres
    :param table_name: name of table in DB
    :param db_schema: DB schema where the table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    db_backend_name = sql_engine.url.get_backend_name().lower()

    _logger.info("Read {db_backend_name} table scheme of '{db_schema}.{table_name}'".format(
        db_backend_name=db_backend_name, db_schema=db_schema, table_name=table_name)
    )

    q1 = sql.SQL("""
    SELECT a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
    FROM pg_attribute a
      JOIN pg_class t ON a.attrelid = t.oid
      JOIN pg_namespace s ON t.relnamespace = s.oid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND t.relname = {table_name}
      AND s.nspname = {db_schema};
    """).format(db_schema=sql.Literal(db_schema),
                table_name=sql.Literal(table_name))

    table_scheme = execute_query_safely(sql_engine=sql_engine, query=q1)

    if table_scheme and len(table_scheme) > 0:
        # Preserve same order of columns in destination table as is in the source one
        table_scheme_cols_order_preserved = _preserve_cols_order_in_fetched_scheme(
            fetched_table_scheme=table_scheme,
            table_name=table_name, db_schema=db_schema,
            sql_engine=sql_engine)

        return table_scheme_cols_order_preserved
    return


def fetch_all_indexes_in_schema(db_schema: str, sql_engine: Engine) -> List:
    """
    This method returns all indexes in schema (if any)
    :param db_schema: DB schema where the table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    q = f"""
    SELECT indexname
    FROM pg_indexes
    WHERE schemaname = '{db_schema}'
    """

    indices_in_schema = pd.read_sql(sql=q, con=sql_engine)['indexname'].tolist()
    return indices_in_schema


def fetch_all_constraints_in_schema(db_schema: str, sql_engine: Engine) -> List:
    """
    This method returns all constraints (such as primary key, foreign key, ...) in schema (if any)
    :param db_schema: DB schema where the table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    q = f"""
    SELECT conname
    FROM   pg_constraint c
    JOIN   pg_namespace n ON n.oid = c.connamespace
    WHERE n.nspname = '{db_schema}'
    """

    constraints_in_schema = pd.read_sql(sql=q, con=sql_engine)['conname'].tolist()
    return constraints_in_schema


def fetch_table_indexes(table_name: str, db_schema: str, sql_engine: Engine) -> pd.DataFrame:
    """
    This method returns indexes of a given table (if any)
    :param table_name: name of table in DB
    :param db_schema: DB schema where the table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    q = f"""
    SELECT schemaname AS schema_name, tablename AS table_name, indexname AS index_name, indexdef AS sql
    FROM pg_indexes
    WHERE schemaname = '{db_schema}'
    AND tablename = '{table_name.strip('"')}'
    """

    table_indices = pd.read_sql(sql=q, con=sql_engine)
    return table_indices


def fetch_table_constraints(table_name: str, db_schema: str, sql_engine: Engine) -> pd.DataFrame:
    """
    This method returns constraints (such as primary key, foreign key, ...) of a given table (if any)
    :param table_name: name of table in DB
    :param db_schema: DB schema where the table is located
    :param sql_engine: SQL engine for postgres
    :return:
    """

    q = f"""
    WITH dd AS (
      SELECT n.nspname AS schema_name, conrelid::regclass::text AS table_name, conname AS constraint_name, 
      pg_get_constraintdef(c.oid) AS sql
      FROM   pg_constraint c
      JOIN   pg_namespace n ON n.oid = c.connamespace
    )
    SELECT * FROM dd
    WHERE schema_name = '{db_schema}'
    AND table_name = '{table_name}'
    """

    table_constraints = pd.read_sql(sql=q, con=sql_engine)
    return table_constraints


def create_empty_table_in_db_using_dtypes(table_name: str, schema: str, table_dtypes: Dict[str, str],
                                          sql_engine: Engine):
    """
    This method creates empty table in DB given a dict with column names as keys and dtypes as values.
    :param table_name: name of table to be created
    :param schema: schema where the table should be copied to
    :param table_dtypes: dict with column names as keys and dtypes as values
    :param sql_engine: SQL engine (redshift, postgres)
    :return:
    """

    query = sql.SQL("""
    DROP TABLE IF EXISTS {schema}.{table_name};
    CREATE TABLE {schema}.{table_name}
    (
    {table_scheme}
    );
    """).format(schema=sql.Identifier(schema), table_name=sql.Identifier(table_name),
                table_scheme=sql.SQL(',\n'.join([' '.join([k, v]) for k, v in table_dtypes.items()])))
    query = query.as_string(sql_engine.raw_connection().cursor())

    _logger.info("Creating empty '{schema}.{table_name}' table in DB".format(
        schema=schema, table_name=table_name))

    execute_query(sql_engine=sql_engine, query=query)
    _logger.debug("Table created")


def remap_hosts(postgres_source_engine_config: Dict) -> Dict:
    """
    This method maps host name into corresponding ip address of RDS instance
    :param postgres_source_engine_config:
    :return:
    """
    if postgres_source_engine_config['host'] == 'mapdata.cluster-ro-c641mhev6x2v.us-east-1.rds.amazonaws.com':
        postgres_source_engine_config['host'] = '172.31.17.15'
    return postgres_source_engine_config


@timing
def copy_table_from_postgres_to_postgres(postgres_source_table_name: str,
                                         postgres_source_schema: str,
                                         postgres_destination_schema: str,
                                         postgres_source_engine: Engine,
                                         postgres_destination_engine: Engine,
                                         cols_to_copy: List[str] = [],
                                         postgres_destination_table_name: str = None,
                                         limit_n_records: Union[int, None] = None):
    """
    This method is used to copy any table from one postgres cluster to another with dtypes preservation.
    :param postgres_source_table_name: name of table in source postgres DB
    :param postgres_source_schema: name of schema where source postgres table is located
    :param postgres_destination_schema: name of schema where the source postgres table should be copied to
    :param postgres_source_engine: postgres SQL engine to connect to DB where the source table is located
                                   (output of get_postgres_engine() method)
    :param postgres_destination_engine: postgres SQL engine to connect to DB where the source table should be
                                        copied to (output of get_postgres_engine() method)
    :param cols_to_copy: columns to be copied (if cols_to_copy = [] -> it will use all columns)
    :param postgres_destination_table_name: name of postgres destination table (where the source table to be copied)
    :param limit_n_records: number of records from source table to be copied to destination table
    :return:
    """

    # Check if source table exists
    table_exists_not_empty = check_table_exists_and_not_empty(
        table_name=postgres_source_table_name,
        schema=postgres_source_schema,
        sql_engine=postgres_source_engine)

    if table_exists_not_empty:

        if not postgres_destination_table_name:
            postgres_destination_table_name = postgres_source_table_name

        postgres_source_engine_config = postgres_source_engine.url.translate_connect_args()

        # Create extensions
        execute_query(query="CREATE EXTENSION IF NOT EXISTS postgres_fdw;", sql_engine=postgres_destination_engine)
        execute_query(query="CREATE EXTENSION IF NOT EXISTS dblink;", sql_engine=postgres_destination_engine)

        # Remap hosts (there are some problems to read by host name - we need ip address)
        postgres_source_engine_config = remap_hosts(postgres_source_engine_config)

        # This is used by dblink extension
        host_string = f"host={postgres_source_engine_config['host']} " \
                      f"port={postgres_source_engine_config['port']} " \
                      f"dbname={postgres_source_engine_config['database']} " \
                      f"user={postgres_source_engine_config['username']} " \
                      f"password={postgres_source_engine_config['password']}"

        # Read table schema from source table
        table_scheme = fetch_table_scheme(
            table_name=postgres_source_table_name,
            db_schema=postgres_source_schema,
            sql_engine=postgres_source_engine
        )
        table_scheme_source = {c[0]: c[1] for c in table_scheme}

        # Limit table scheme to columns of interest (if applicable)
        if cols_to_copy:
            table_scheme_source = {c: v for c, v in table_scheme_source.items() if c in cols_to_copy}
            table_scheme_source_formatted = ",\n".join([" ".join([c[0], c[1]])
                                                        for c in table_scheme if c[0] in cols_to_copy])

        else:
            table_scheme_source_formatted = ",\n".join([" ".join([c[0], c[1]]) for c in table_scheme])

        table_scheme_destination = table_scheme_source.copy()

        # Create schema in destination DB
        execute_query(query=f"CREATE SCHEMA IF NOT EXISTS {postgres_destination_schema}",
                      sql_engine=postgres_destination_engine)

        # Create empty table in destination DB based on source scheme and selected columns to be copied
        create_empty_table_in_db_using_dtypes(
            table_name=postgres_destination_table_name,
            schema=postgres_destination_schema,
            table_dtypes=table_scheme_destination,
            sql_engine=postgres_destination_engine
        )

        # These columns to be inserted to destination table
        if cols_to_copy:
            cols_to_insert = ", ".join(cols_to_copy)
        else:
            cols_to_insert = ", ".join([c[0] for c in table_scheme])

        cols_to_copy = ", ".join(cols_to_copy) if cols_to_copy else "*"

        # Inserting data from source table to the destination table
        q = f"""
        INSERT INTO {postgres_destination_schema}.{postgres_destination_table_name} (
          {cols_to_insert}
        )
        SELECT {cols_to_copy}
        FROM dblink(
          '{host_string}',
          'SELECT {cols_to_copy} 
          FROM {postgres_source_schema}.{postgres_source_table_name}
        """

        if limit_n_records:
            q += f"\nLIMIT {limit_n_records}\n"

        q += f"""
        ') 
        AS origin_data (
           {table_scheme_source_formatted}
        );
        """

        q = prettify_query_outlook(q)
        execute_query(query=q, sql_engine=postgres_destination_engine)

        # Adding constraints / indices
        table_constraints = fetch_table_constraints(table_name=postgres_source_table_name,
                                                    db_schema=postgres_source_schema,
                                                    sql_engine=postgres_source_engine)

        table_indices = fetch_table_indexes(table_name=postgres_source_table_name,
                                            db_schema=postgres_source_schema,
                                            sql_engine=postgres_source_engine)

        # Find if there is any intersection between names of constraints and indices
        # - Normally, the `primary key` in constraints == `CREATE UNIQUE INDEX` in indices
        # - we will process such entry only once - will add it as a primary key
        intersection_constrains_and_index = list(
            set(table_constraints['constraint_name']).intersection(set(table_indices['index_name']))
        )

        if intersection_constrains_and_index:
            _logger.info(f"There are {len(intersection_constrains_and_index)} constrains that were also found in the "
                         f"table of indices: {intersection_constrains_and_index}")
            mask = table_indices['index_name'].isin(intersection_constrains_and_index)
            table_indices = table_indices[~mask].reset_index(drop=True)

        if not table_constraints.empty:
            _logger.info(f"Adding constraints to {postgres_destination_schema}.{postgres_destination_table_name}")

            all_constraints_in_destination_schema = fetch_all_constraints_in_schema(
                db_schema=postgres_destination_schema,
                sql_engine=postgres_destination_engine)

            for constraint_name, constraint_sql in table_constraints[['constraint_name', 'sql']].values.tolist():

                if constraint_name in all_constraints_in_destination_schema:
                    # New name for constraint
                    constraint_name_new = f'{constraint_name}_copy'

                    _logger.info(f"Constraint '{constraint_name}' already exists in the destination schema. "
                                 f"Will create '{constraint_name_new}' instead")

                    q = f"""
                    ALTER TABLE {postgres_destination_schema}.{postgres_destination_table_name}
                       ADD CONSTRAINT {constraint_name_new}
                       {constraint_sql};
                    """
                else:
                    q = f"""
                    ALTER TABLE {postgres_destination_schema}.{postgres_destination_table_name}
                       ADD CONSTRAINT {constraint_name}
                       {constraint_sql};
                    """

                execute_query(sql_engine=postgres_destination_engine, query=q)

        if not table_indices.empty:
            _logger.info(f"Adding indexes to {postgres_destination_schema}.{postgres_destination_table_name}")

            all_indices_in_destination_schema = fetch_all_indexes_in_schema(db_schema=postgres_destination_schema,
                                                                            sql_engine=postgres_destination_engine)

            for index_name, index_sql in table_indices[['index_name', 'sql']].values.tolist():
                if index_name in all_indices_in_destination_schema:
                    # New name for index
                    index_name_new = f'{index_name}_copy'
                    index_sql_new = index_sql.replace(index_name, index_name_new)

                    _logger.info(f"Index '{index_name}' already exists in the destination schema. "
                                 f"Will create '{index_name_new}' instead")

                    q = index_sql_new
                else:
                    q = index_sql
            execute_query(sql_engine=postgres_destination_engine, query=q)


def copy_delta_table_from_postgres_to_postgres(postgres_source_table_name: str,
                                               postgres_source_schema: str,
                                               postgres_destination_schema: str,
                                               postgres_source_engine: Engine,
                                               postgres_destination_engine: Engine,
                                               id_col: str,
                                               time_col: str,
                                               postgres_destination_table_name: str = None,
                                               limit_n_records: Union[int, None] = None):
    """
    This method is used to sync up the two tables in different RDS: e.g. one need to add the most recent data from prod
    to staging / dev environment. This method relies on id_col (which is of incremental serial type) when computing
    deltas to be copied.

    :param postgres_source_table_name: name of table in source postgres DB
    :param postgres_source_schema: name of schema where source postgres table is located
    :param postgres_destination_schema: name of schema where the source postgres table should be copied to
    :param postgres_source_engine: postgres SQL engine to connect to DB where the source table is located
                                   (output of get_postgres_engine() method)
    :param postgres_destination_engine: postgres SQL engine to connect to DB where the source table should be
                                        copied to (output of get_postgres_engine() method)
    :param id_col: column that is used as a unique identifier in the table (serial, incremental)
    :param time_col: column that is representing time of the data (ingestion, event datetime, etc..)
    :param postgres_destination_table_name: name of postgres destination table (where the source table to be copied)
    :param limit_n_records: number of records from source table to be copied to destination table
    :return:
    """

    # Check if source table exists
    table_exists_not_empty = check_table_exists_and_not_empty(
        table_name=postgres_source_table_name,
        schema=postgres_source_schema,
        sql_engine=postgres_source_engine)

    if table_exists_not_empty:

        if not postgres_destination_table_name:
            postgres_destination_table_name = postgres_source_table_name

        postgres_source_engine_config = postgres_source_engine.url.translate_connect_args()

        # Create extensions
        execute_query(query="CREATE EXTENSION IF NOT EXISTS postgres_fdw;", sql_engine=postgres_destination_engine)
        execute_query(query="CREATE EXTENSION IF NOT EXISTS dblink;", sql_engine=postgres_destination_engine)

        # Remap hosts (there are some problems to read by host name - we need ip address)
        postgres_source_engine_config = remap_hosts(postgres_source_engine_config)

        # This is used by dblink extension
        host_string = f"host={postgres_source_engine_config['host']} " \
                      f"port={postgres_source_engine_config['port']} " \
                      f"dbname={postgres_source_engine_config['database']} " \
                      f"user={postgres_source_engine_config['username']} " \
                      f"password={postgres_source_engine_config['password']}"

        # Read table schema from source table
        table_scheme = fetch_table_scheme(
            table_name=postgres_source_table_name,
            db_schema=postgres_source_schema,
            sql_engine=postgres_source_engine
        )

        # Limit table scheme to columns of interest (if applicable)
        table_scheme_source_formatted = ",\n".join([" ".join([c[0], c[1]]) for c in table_scheme])

        # These columns to be inserted to destination table
        cols_to_insert = ", ".join([c[0] for c in table_scheme])

        # Get number of records in the source and destination tables as well as the time of the latest data point
        q1 = f"""
        SELECT COUNT(*) AS cnt, max({time_col}) AS {time_col}_max 
        FROM {postgres_source_schema}.{postgres_source_table_name}
        """

        source_stats = pd.read_sql(sql=q1, con=postgres_source_engine)
        cnt_source = source_stats['cnt'].iloc[0]
        max_time_source = source_stats[time_col + '_max'].iloc[0]

        destination_stats = pd.read_sql(sql=q1, con=postgres_destination_engine)
        cnt_destination = destination_stats['cnt'].iloc[0]
        max_time_destination = destination_stats[time_col + '_max'].iloc[0]

        delta_n_records = cnt_source - cnt_destination

        _logger.info(f"Size of `{postgres_source_table_name}` (source / destination): {cnt_source} / {cnt_destination}")
        _logger.info(f"- Time: {max_time_source} / {max_time_destination}")

        if delta_n_records > 0:
            _logger.info(f"- Delta records to copy: {delta_n_records}")

            # Get event_id of the latest data point in the source and destination tables
            q2 = f"""
            SELECT max({id_col}) as {id_col}_max 
            FROM {postgres_source_schema}.{postgres_source_table_name}
            """

            event_id_source = execute_query(sql_engine=postgres_source_engine,
                                            query=q2,
                                            print_response=False)[0][0]

            event_id_destination = execute_query(sql_engine=postgres_destination_engine,
                                                 query=q2,
                                                 print_response=False)[0][0]

            if not event_id_destination:
                _logger.info(f"Destination table exists but empty!!! All data from source table will be copied!")
                event_id_destination = 0
                
            q3 = f"""
            SELECT COUNT(*) AS cnt
            FROM {postgres_source_schema}.{postgres_source_table_name}
            WHERE {id_col} > {event_id_destination}
            """

            delta_to_copy = execute_query(sql_engine=postgres_source_engine,
                                          query=q3,
                                          print_response=False)[0][0]

            _logger.info(f"Max value of `{id_col}` in `{postgres_source_table_name}` (source / destination): "
                         f"{event_id_source} / {event_id_destination}.")
            _logger.info(f"- Delta records: {delta_to_copy}\n")

            # Inserting data from source table to the destination table
            q4 = f"""
            INSERT INTO {postgres_destination_schema}.{postgres_destination_table_name} (
              {cols_to_insert}
            )
            SELECT *
            FROM dblink(
              '{host_string}',
              'SELECT * 
              FROM {postgres_source_schema}.{postgres_source_table_name}
              WHERE {id_col} > {event_id_destination}
            """

            if limit_n_records:
                q4 += f"\nLIMIT {limit_n_records}\n"

            q4 += f"""
            ') 
            AS origin_data (
               {table_scheme_source_formatted}
            );
            """

            q4 = prettify_query_outlook(q4)
            execute_query(query=q4, sql_engine=postgres_destination_engine)

            # Check stats on destination table after sync-up procedure
            destination_stats = pd.read_sql(sql=q1, con=postgres_destination_engine)
            cnt_destination = destination_stats['cnt'].iloc[0]
            max_time_destination = destination_stats[time_col + '_max'].iloc[0]

            _logger.info(f">>> After the sync-up with the source table:")
            _logger.info(f"- Size of `{postgres_source_table_name}` (source / destination): "
                         f"{cnt_source} / {cnt_destination}")
            _logger.info(f"- Time: {max_time_source} / {max_time_destination}")

        elif delta_n_records < 0:
            _logger.info(f"- Destination table have more records than source ({cnt_destination} > {cnt_source})")

        else:
            _logger.info(f"- Tables have the same number of records - nothing to copy ...")


def copy_data_from_s3_to_postgres(filename: str, path_output_dir_s3: str, postgres_table: str, postgres_schema: str,
                                  postgres_engine: Engine, s3_resource: Union[ServiceResource, None] = None,
                                  region: str = 'us-east-1', delimiter: str = "|") -> None:

    if not s3_resource:
        s3_resource = boto3.resource('s3', region_name=region)

    files_to_load_to_postgres = list_file_objs_in_s3_dir(s3_resource=s3_resource,
                                                         path_output_dir_s3=path_output_dir_s3,
                                                         include_dir_name=False)

    files_to_load_to_postgres = sorted([obj.key.split('/')[-1]
                                        for obj in files_to_load_to_postgres if filename in obj.key])

    template = """
    SELECT aws_s3.table_import_from_s3(
       '{postgres_schema}.{postgres_table}',
       '',
       'DELIMITER ''{delimiter}''',
       aws_commons.create_s3_uri('{path_output_dir_s3}', '{filename}', '{region}')
    );
    """

    for f in files_to_load_to_postgres:
        q = template.format(postgres_schema=postgres_schema, postgres_table=postgres_table, delimiter=delimiter,
                            path_output_dir_s3=path_output_dir_s3, filename=f, region=region)
        execute_query(sql_engine=postgres_engine, query=q)

    return


def initialize_elastic_search_engine(config: dict) -> OpenSearchClient:
    """
    Loading config file for Elastic Search (ES) DB from AWS Secrets Manager and initializing ES engine
    :return:
    """

    host = config['host']
    port = config['port']
    region = config['region']
    service = config['service']

    credentials = boto3.Session().get_credentials()

    assert credentials.access_key and credentials.secret_key, "Run `aws configure` in your shell to set-up access " \
                                                              "to AWS resources"
    aws_auth = AWS4Auth(credentials.access_key,
                        credentials.secret_key,
                        region,
                        service,
                        credentials.token)

    es_client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        max_retries=10,
        retry_on_timeout=True,
        request_timeout=30
    )

    time.sleep(1)
    assert es_client.ping()
    return es_client


def compose_elastic_search_index_name(table_name: str, schema: Union[str, None]) -> str:
    """
    This method is used to create Elastic Search index name from table name and the schema name (if provided)
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    """
    assert table_name, "Table name should always be provided"
    return f"{schema}.{table_name}" if schema else table_name


def drop_elastic_search_index(es_client: OpenSearchClient, table_name: str, schema: Union[str, None]) -> None:
    """
    This method drops Elastic Search index if that exists
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    :return:
    """
    es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)
    es_client.indices.delete(index=es_index_name, ignore=[400, 404])


def get_list_of_elastic_search_indices(es_client: OpenSearchClient) -> List:
    """
    This method returns a list of Elastic Search indices (aka tables)
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :return:
    """
    return list(es_client.indices.get_alias("*").keys())


def check_elastic_search_index_exists_and_not_empty(es_client: OpenSearchClient, table_name: str,
                                                    schema: Union[str, None]) -> bool:
    """
    This method checks whether table exists in the given schema
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    :return:
    """

    try:
        es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)
        response = int(es_client.cat.count(index=es_index_name,
                                           params={"format": "json"})[0]['count'])
        return True if response and response > 0 else False
    except exceptions.NotFoundError:
        return False


def get_data_from_elastic_search_index(es_client: OpenSearchClient, table_name: str, schema: Union[str, None],
                                       n_records: Union[int, None] = 5) -> Dict:
    """
    This method takes N records from Elastic Search index if that exists
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    :param n_records: how many records to return (None - return all)
    :return:
    """
    query = {
        "query": {
            "match_all": {}
        }
    }
    if n_records:
        query["size"] = n_records

    es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)

    es_index_exists_and_not_empty = check_elastic_search_index_exists_and_not_empty(
        es_client=es_client, table_name=table_name, schema=schema,
    )

    if es_index_exists_and_not_empty:
        response = es_client.search(body=query, index=es_index_name)
    else:
        response = {}
    return response


def get_size_of_elastic_search_index(es_client: OpenSearchClient, table_name: str, schema: Union[str, None]) -> int:
    """
    This method returns number of records in Elastic Search index
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    :return: number of records in Elastic Search index
    """
    # Compose full name of ES index
    es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)
    return int(es_client.cat.count(index=es_index_name, params={"format": "json"})[0]['count'])


@timing
def load_pandas_df_to_elastic_search(es_client: OpenSearchClient, df: pd.DataFrame, table_name: str,
                                     schema: Union[str, None], recreate_index_if_exists: bool = False,
                                     chunk_size: int = 500, max_chunk_bytes: int = 104857600,
                                     request_timeout: int = 30, n_threads: int = -1) -> None:
    """
    This method is used to load data from pandas DF to Elastic Search
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param df: pandas DF where first column to be used as KEY in Elastic Search
    :param table_name: name of table to be used in Elastic Search index
    :param schema: name of schema to be used in Elastic Search index
    :param recreate_index_if_exists: if True - recreates ES index
    :param chunk_size: number of docs in one chunk sent to client (default: 500)
    :param max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :param request_timeout: time out of request [seconds]
    :param n_threads: size of the thread pool to use for the bulk requests
    :return:
    """

    # Compose full name of ES index
    es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)

    # Check Elastic index exists and not empty
    es_index_exists_and_not_empty = check_elastic_search_index_exists_and_not_empty(
        es_client=es_client, table_name=table_name, schema=schema,
    )

    if not es_index_exists_and_not_empty or recreate_index_if_exists:
        _logger.info('Start loading DF to Elastic Search')

        # Drop elastic search index if that exists
        if es_index_exists_and_not_empty:
            drop_elastic_search_index(es_client=es_client, table_name=table_name, schema=schema)

        # Size of the thread pool to use for the bulk requests
        n_threads = cpu_count() if n_threads == -1 else n_threads  # number of CPU cores on your system

        body_of_requests = list(df.to_dict(orient='index').values())
        indices = list(range(1, len(body_of_requests)))
        requests = list(zip(indices, body_of_requests))
        _logger.info(f"Number of records to be uploaded to elastic search: {len(requests)}")
        _logger.info(f"Example of request: {requests[0]}")

        _logger.info("Preparing elastic search documents (to be loaded via bulk load) ...")
        bulk_load_request = []
        for elem in requests:
            i, r = elem
            bulk_load_request.append({'_index': es_index_name, '_id': i, "_source": r})

        _logger.info(f"Parallel bulk load to elastic search index {es_index_name} using {n_threads} ...")
        kwargs = {"request_timeout": request_timeout}
        response = helpers.parallel_bulk(client=es_client, actions=bulk_load_request, thread_count=n_threads,
                                         chunk_size=chunk_size, max_chunk_bytes=max_chunk_bytes,
                                         **kwargs)
        for success, info in response:
            if not success:
                _logger.info('A document failed:', info)


@timing
def unload_elastic_search_index_to_list(es_client: OpenSearchClient, table_name: str, schema: Union[str, None],
                                        chunk_size: int = 500, keep_scroll_context_seconds: int = 2,
                                        n_records_to_unload: Union[int, None] = None, verbose: bool = False) -> List:
    """
    This method is used to unload elastic search index to list
    :param es_client: ES client (output of initialize_elastic_search_engine() method)
    :param table_name: name of table in Elastic Search to unload
    :param schema: name of schema where the table is located
    :param chunk_size: number of docs in one chunk to unload (default: 500)
    :param keep_scroll_context_seconds: specify how long a consistent view of the index should be maintained
                                        for scrolled search (in seconds)
    :param n_records_to_unload: limit number of records to be unloaded from index (if None -> unload all data)
    :param verbose: if True - run procedure in the debug mode
    :return:
    """

    # Compose full name of ES index
    es_index_name = compose_elastic_search_index_name(table_name=table_name, schema=schema)

    # Query to unload all data from Elastic Search Index
    query = {
        "size": chunk_size,
        "query": {"match_all": {}}
    }

    # Make a search() request to get all docs in the index
    resp = es_client.search(
        index=es_index_name,
        body=query,
        scroll=f'{keep_scroll_context_seconds}s'  # length of time to keep search context
    )

    # Keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']

    # Use a 'while' iterator to loop over document 'hits'
    es_index_data = []
    doc_count = 0

    while len(resp['hits']['hits']):

        if verbose:
            _logger.info("\nResponse for index:", es_index_name)
            _logger.info("_scroll_id:", resp['_scroll_id'])
            _logger.info(f'response["hits"]["total"]["value"]: {resp["hits"]["total"]["value"]}')

        # Iterate over the document hits for each 'scroll'
        for doc in resp['hits']['hits']:

            if verbose:
                _logger.info("\n", doc['_id'], doc['_source'])

            doc_count += 1
            es_index_data.append(doc)

            if verbose:
                _logger.info("Document #:", doc_count)

        # Make a request using the Scroll API
        resp = es_client.scroll(
            scroll_id=old_scroll_id,
            scroll=f'{keep_scroll_context_seconds}s'  # length of time to keep search context
        )

        # Check if there's a new scroll ID
        if old_scroll_id != resp['_scroll_id']:
            _logger.info("New scroll id:", resp['_scroll_id'])

        # Keep track of pass scroll _id
        old_scroll_id = resp['_scroll_id']

        # If we want to unload just some limited number of documents
        if n_records_to_unload and doc_count > n_records_to_unload:
            break

    # Print the total document count
    _logger.info(f"\nTotal number of documents unloaded: {doc_count}")

    return es_index_data


def create_empty_table_postgres_from_scheme(
    table_name: str,
    postgres_schema: str,
    table_dtypes: Dict[str, str],
    postgres_engine: Engine
):
    """
    This method creates empty table in postgres given a dict with column names as keys and postgres dtypes as values.
    :param table_name: name of table in postgres
    :param postgres_schema: postgres schema where the table should be copied to
    :param table_dtypes: dict with column names as keys and dtypes as values (e.g. output of map_pgs_dtypes_to_postgres)
    :param postgres_engine: postgres SQL engine
    :return:
    """

    query = sql.SQL("""
    DROP TABLE IF EXISTS {postgres_schema}.{table_name};
    CREATE TABLE {postgres_schema}.{table_name}
    (
    {table_scheme}
    );
    """).format(postgres_schema=sql.Identifier(postgres_schema), table_name=sql.Identifier(table_name),
                table_scheme=sql.SQL(',\n'.join([' '.join([k, v]) for k, v in table_dtypes.items()])))
    query = query.as_string(postgres_engine.raw_connection().cursor())

    _logger.info("Creating empty '{postgres_schema}.{table_name}' table on postgres".format(
        postgres_schema=postgres_schema, table_name=table_name))

    execute_query(sql_engine=postgres_engine, query=query)
    _logger.debug("Table created")
