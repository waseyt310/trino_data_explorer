import trino
import logging
from trino.auth import OAuth2Authentication
import urllib3
import pandas as pd
from typing import Optional, List, Any, Dict
from contextlib import contextmanager

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
                 schema: str = 'oltp_business_analytics'):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Trino database"""
        try:
            auth = OAuth2Authentication()
            self.connection = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme='https',
                verify=False,
                auth=auth
            )
            logger.info(f"Successfully connected to Trino database (catalog: {self.catalog})")
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise

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
        """Test the database connection"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(results, columns=columns)
                return pd.DataFrame(results)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise

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