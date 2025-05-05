import trino
import logging
from trino.auth import OAuth2Authentication
import urllib3
import pandas as pd
from typing import Optional, List, Any, Dict
from contextlib import contextmanager
import time
import random
import os
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TrinoConnection:
    def __init__(self, host: str = 'trino.ops.eks.prod01.tk.dev',
                 port: int = 443,
                 user: str = 'waseyt.ibrahim',
                 catalog: str = 'oltp_business_analytics',
                 schema: str = 'oltp_business_analytics',
                 connect_timeout: int = 60,
                 request_timeout: int = 60,
                 max_attempts: int = 5,
                 client_id: str = 'trino-client',
                 redirect_port: int = 8080):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.max_attempts = max_attempts
        self.client_id = client_id
        self.redirect_port = redirect_port
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Trino database with retry mechanism"""
        attempt = 0
        last_exception = None
        
        while attempt < self.max_attempts:
            try:
                # Exponential backoff with jitter
                if attempt > 0:
                    backoff_time = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.info(f"Retrying connection in {backoff_time:.2f} seconds (attempt {attempt+1}/{self.max_attempts})")
                    time.sleep(backoff_time)
                
                # Configure OAuth2 authentication
                base_url = f"https://{self.host}:{self.port}"
                redirect_uri = f"http://localhost:{self.redirect_port}/oauth2/callback"
                
                # Configure OAuth2 with appropriate endpoints
                auth = OAuth2Authentication(
                    client_id=self.client_id,
                    redirect_uri=redirect_uri,
                    token_endpoint=urljoin(base_url, "/oauth2/token"),
                    authorization_endpoint=urljoin(base_url, "/oauth2/authorize"),
                    cache_token=True
                )
                
                # Set the HTTP headers for client info
                http_headers = {
                    'X-Trino-Client-Info': 'Trino Data Explorer',
                    'X-Trino-Client-Tags': 'streamlit-app'
                }
                
                # Configure connection with timeout settings
                self.connection = trino.dbapi.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                    http_scheme='https',
                    verify=False,
                    auth=auth,
                    request_timeout=self.request_timeout,
                    http_headers=http_headers,
                    # Note: connect_timeout is handled by the underlying requests library
                    # and is not directly configurable in trino.dbapi.connect
                )
                logger.info(f"Successfully connected to Trino database (catalog: {self.catalog})")
                return  # Connection successful, exit the retry loop
            except urllib3.exceptions.ConnectTimeoutError as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt+1}/{self.max_attempts} timed out: {str(e)}")
            except Exception as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt+1}/{self.max_attempts} failed: {str(e)}")
            
            attempt += 1
        
        # If we've exhausted all retries, raise the last exception
        error_msg = f"Failed to connect to Trino after {self.max_attempts} attempts"
        if last_exception:
            error_msg += f": {str(last_exception)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    @contextmanager
    def get_cursor(self):
        """Context manager for cursor handling"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            yield cursor
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {str(e)}")

    def test_connection(self) -> bool:
        """Test the database connection with retry mechanism"""
        attempt = 0
        while attempt < self.max_attempts:
            try:
                # Exponential backoff with jitter for retries
                if attempt > 0:
                    backoff_time = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.info(f"Retrying connection test in {backoff_time:.2f} seconds (attempt {attempt+1}/{self.max_attempts})")
                    time.sleep(backoff_time)
                
                with self.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result and result[0] == 1:
                        if attempt > 0:
                            logger.info(f"Connection test succeeded after {attempt+1} attempts")
                        return True
            except Exception as e:
                error_type = type(e).__name__
                if "TimeoutError" in error_type or "ConnectTimeout" in error_type:
                    logger.warning(f"Connection test attempt {attempt+1}/{self.max_attempts} timed out: {str(e)}")
                else:
                    logger.warning(f"Connection test attempt {attempt+1}/{self.max_attempts} failed: {str(e)}")
            
            attempt += 1
        
        logger.error(f"Connection test failed after {self.max_attempts} attempts")
        return False

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame with retry mechanism"""
        attempt = 0
        last_exception = None
        
        while attempt < self.max_attempts:
            try:
                # Exponential backoff with jitter for retries
                if attempt > 0:
                    backoff_time = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.info(f"Retrying query in {backoff_time:.2f} seconds (attempt {attempt+1}/{self.max_attempts})")
                    time.sleep(backoff_time)
                
                with self.get_cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        return pd.DataFrame(results, columns=columns)
                    return pd.DataFrame(results)
            except urllib3.exceptions.TimeoutError as e:
                last_exception = e
                logger.warning(f"Query execution attempt {attempt+1}/{self.max_attempts} timed out: {str(e)}")
            except Exception as e:
                last_exception = e
                # Check if this is a connection error that we should retry
                error_str = str(e).lower()
                if "timeout" in error_str or "connection" in error_str:
                    logger.warning(f"Query execution attempt {attempt+1}/{self.max_attempts} failed with connection error: {str(e)}")
                else:
                    # For other errors, don't retry
                    logger.error(f"Query execution failed: {str(e)}")
                    raise
            
            attempt += 1
            
            # If connection was lost, try to reconnect before next attempt
            if self.connection is None or not self.test_connection():
                logger.info("Reconnecting to Trino before retry")
                try:
                    self._connect()
                except Exception as connect_err:
                    logger.error(f"Failed to reconnect: {str(connect_err)}")
        
        # If we've exhausted all retries
        error_msg = f"Query execution failed after {self.max_attempts} attempts"
        if last_exception:
            error_msg += f": {str(last_exception)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of tables in the specified schema"""
        schema = schema or self.schema
        query = f"""
        SELECT table_name 
        FROM {self.catalog}.information_schema.tables 
        WHERE table_schema = '{schema}'
        """
        try:
            df = self.execute_query(query)
            return df['table_name'].tolist()
        except Exception as e:
            logger.error(f"Failed to get tables: {str(e)}")
            raise

    def close(self) -> None:
        """Close the database connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.connection = None
    
    def switch_catalog(self, new_catalog: str, new_schema: Optional[str] = None) -> bool:
        """
        Switch to a different catalog and optionally a different schema
        
        Args:
            new_catalog (str): The catalog to switch to
            new_schema (Optional[str]): The schema to switch to (defaults to same name as catalog)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.close()
            self.catalog = new_catalog
            if new_schema:
                self.schema = new_schema
            else:
                self.schema = new_catalog  # Default schema to same name as catalog
            self._connect()
            return self.test_connection()
        except Exception as e:
            logger.error(f"Failed to switch catalog: {str(e)}")
            return False
    
    def get_all_catalogs(self) -> List[str]:
        """
        Get a list of all available catalogs
        
        Returns:
            List[str]: List of catalog names
        """
        try:
            query = "SHOW CATALOGS"
            df = self.execute_query(query)
            return df['Catalog'].tolist()
        except Exception as e:
            logger.error(f"Failed to get catalogs: {str(e)}")
            return []

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

def main():
    """Test the connection and basic functionality"""
    with TrinoConnection() as conn:
        if conn.test_connection():
            logger.info("Connection test successful!")
            tables = conn.get_tables()
            logger.info(f"Available tables: {tables}")

if __name__ == "__main__":
    main() 