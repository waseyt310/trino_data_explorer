import trino
import logging
from trino.auth import BasicAuthentication
import urllib3
import pandas as pd
from typing import Optional, List, Any, Dict, Tuple
from contextlib import contextmanager
import time
import random
import os
import socket
from urllib.parse import urljoin
import requests
import paramiko
import threading
import atexit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Default values for proxy configuration
DEFAULT_PROXY_HOST = None  # Set to your proxy server host if needed
DEFAULT_PROXY_PORT = None  # Set to your proxy server port if needed

# Environment variable names for proxy configuration
ENV_PROXY_HOST = "TRINO_PROXY_HOST"
ENV_PROXY_PORT = "TRINO_PROXY_PORT"
ENV_HTTP_PROXY = "HTTP_PROXY"
ENV_HTTPS_PROXY = "HTTPS_PROXY"
ENV_NO_PROXY = "NO_PROXY"

# Environment variable names for SSH tunnel configuration
ENV_SSH_HOST = "TRINO_SSH_HOST"
ENV_SSH_PORT = "TRINO_SSH_PORT"
ENV_SSH_USERNAME = "TRINO_SSH_USERNAME"
ENV_SSH_PASSWORD = "TRINO_SSH_PASSWORD"
ENV_SSH_KEY_FILE = "TRINO_SSH_KEY_FILE"
ENV_SSH_KEY_PASSPHRASE = "TRINO_SSH_KEY_PASSPHRASE"
ENV_SSH_LOCAL_PORT = "TRINO_SSH_LOCAL_PORT"

# Default local port range for SSH tunneling
DEFAULT_SSH_LOCAL_PORT_START = 10000
DEFAULT_SSH_LOCAL_PORT_END = 10100

def check_host_connectivity(host: str, port: int, timeout: int = 5) -> bool:
    """Check if the host is reachable by attempting a socket connection"""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as e:
        logger.warning(f"Host connectivity check failed for {host}:{port}: {str(e)}")
        return False

def find_available_port(start: int = DEFAULT_SSH_LOCAL_PORT_START, end: int = DEFAULT_SSH_LOCAL_PORT_END) -> int:
    """Find an available local port in the specified range"""
    for port in range(start, end):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"No available ports in range {start}-{end}")

class SSHTunnel:
    """
    SSH tunnel for port forwarding between local machine and remote server.
    This allows connecting to a Trino server behind a firewall by tunneling
    through an SSH server that has access to the Trino server.
    """
    def __init__(
        self,
        ssh_host: str,
        ssh_port: int = 22,
        ssh_username: str = None,
        ssh_password: str = None,
        ssh_key_file: str = None,
        ssh_key_passphrase: str = None,
        remote_host: str = 'localhost',
        remote_port: int = 443,
        local_port: int = None
    ):
        """
        Initialize an SSH tunnel for port forwarding.
        
        Args:
            ssh_host: The SSH server hostname
            ssh_port: The SSH server port (default 22)
            ssh_username: Username for SSH authentication
            ssh_password: Password for SSH authentication (if not using key)
            ssh_key_file: Path to private key file (if not using password)
            ssh_key_passphrase: Passphrase for private key (if needed)
            remote_host: Remote host to connect to via SSH tunnel
            remote_port: Remote port to connect to
            local_port: Local port to use for forwarding (optional)
        """
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_key_file = ssh_key_file
        self.ssh_key_passphrase = ssh_key_passphrase
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_port = local_port or find_available_port()
        
        self.client = None
        self.tunnel_thread = None
        self.running = False
        self.lock = threading.Lock()
        self._register_atexit()
        
    def _register_atexit(self):
        """Register cleanup function to ensure tunnel is closed on exit"""
        atexit.register(self.close)
    
    def start(self) -> int:
        """
        Start the SSH tunnel with port forwarding.
        
        Returns:
            int: The local port number to connect to
        """
        if self.running:
            return self.local_port
            
        logger.info(f"Starting SSH tunnel to {self.ssh_host}:{self.ssh_port}")
        try:
            # Create SSH client
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Configure authentication
            connect_kwargs = {
                'hostname': self.ssh_host,
                'port': self.ssh_port,
                'timeout': 30
            }
            
            if self.ssh_username:
                connect_kwargs['username'] = self.ssh_username
            
            if self.ssh_password:
                connect_kwargs['password'] = self.ssh_password
            
            if self.ssh_key_file:
                if os.path.exists(self.ssh_key_file):
                    connect_kwargs['key_filename'] = self.ssh_key_file
                    if self.ssh_key_passphrase:
                        connect_kwargs['passphrase'] = self.ssh_key_passphrase
                else:
                    logger.warning(f"SSH key file not found: {self.ssh_key_file}")
                    if self.ssh_password:
                        logger.info("Falling back to password authentication")
                    else:
                        raise FileNotFoundError(f"SSH key file not found: {self.ssh_key_file}")
            
            # Connect to SSH server
            self.client.connect(**connect_kwargs)
            
            # Start port forwarding in a background thread
            def forward_tunnel():
                with self.lock:
                    self.running = True
                
                try:
                    # This blocks until the tunnel is closed
                    self.client.get_transport().request_port_forward(
                        '127.0.0.1', self.local_port, self.remote_host, self.remote_port
                    )
                    
                    while self.running:
                        time.sleep(1)
                except Exception as e:
                    logger.error(f"SSH tunnel error: {str(e)}")
                finally:
                    with self.lock:
                        self.running = False
            
            # Start forwarding in a separate thread
            self.tunnel_thread = threading.Thread(target=forward_tunnel, daemon=True)
            self.tunnel_thread.start()
            
            # Wait briefly to ensure tunnel is established
            time.sleep(1)
            
            # Check if the tunnel is running
            if not self.running:
                raise Exception("Failed to establish SSH tunnel")
                
            logger.info(f"SSH tunnel established - forwarding localhost:{self.local_port} to {self.remote_host}:{self.remote_port}")
            return self.local_port
            
        except Exception as e:
            logger.error(f"Error establishing SSH tunnel: {str(e)}")
            self.close()
            raise
    
    def is_active(self) -> bool:
        """Check if the tunnel is active"""
        with self.lock:
            return self.running and self.client and self.client.get_transport() and self.client.get_transport().is_active()
    
    def close(self) -> None:
        """Close the SSH tunnel"""
        with self.lock:
            self.running = False
        
        if self.client:
            try:
                logger.info("Closing SSH tunnel")
                self.client.close()
            except Exception as e:
                logger.warning(f"Error closing SSH tunnel: {str(e)}")
            finally:
                self.client = None
        
        if self.tunnel_thread and self.tunnel_thread.is_alive():
            self.tunnel_thread.join(timeout=2)
            self.tunnel_thread = None

class TrinoConnection:
    def __init__(self, host: str = 'trino.ops.eks.prod01.tk.dev',
                 port: int = 443,
                 user: str = 'waseyt.ibrahim',
                 catalog: str = 'oltp_business_analytics',
                 schema: str = 'oltp_business_analytics',
                 connect_timeout: int = 60,
                 request_timeout: int = 60,
                 max_attempts: int = 5,
                 proxy_host: Optional[str] = None,
                 proxy_port: Optional[int] = None,
                 ssh_host: Optional[str] = None,
                 ssh_port: int = 22,
                 ssh_username: Optional[str] = None,
                 ssh_password: Optional[str] = None,
                 ssh_key_file: Optional[str] = None,
                 ssh_key_passphrase: Optional[str] = None,
                 ssh_local_port: Optional[int] = None):
        """
        Initialize a connection to a Trino database with support for SSH tunneling and proxies.
        
        Args:
            host: Trino server hostname
            port: Trino server port
            user: Username for Trino authentication
            catalog: Default catalog to connect to
            schema: Default schema to use
            connect_timeout: Connection timeout in seconds
            request_timeout: Request timeout in seconds
            max_attempts: Maximum number of connection attempts
            proxy_host: HTTP proxy hostname (optional)
            proxy_port: HTTP proxy port (optional)
            ssh_host: SSH server hostname for tunneling (optional)
            ssh_port: SSH server port (default 22)
            ssh_username: Username for SSH authentication
            ssh_password: Password for SSH authentication
            ssh_key_file: Path to SSH private key file
            ssh_key_passphrase: Passphrase for SSH private key
            ssh_local_port: Local port to use for SSH tunnel
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.max_attempts = max_attempts
        
        # Configure proxy settings from parameters or environment variables
        self.proxy_host = proxy_host or os.environ.get(ENV_PROXY_HOST, DEFAULT_PROXY_HOST)
        self.proxy_port = proxy_port or os.environ.get(ENV_PROXY_PORT, DEFAULT_PROXY_PORT)
        if self.proxy_port and isinstance(self.proxy_port, str):
            self.proxy_port = int(self.proxy_port)
            
        # Get proxy configuration from standard environment variables
        self.http_proxy = os.environ.get(ENV_HTTP_PROXY)
        self.https_proxy = os.environ.get(ENV_HTTPS_PROXY)
        self.no_proxy = os.environ.get(ENV_NO_PROXY)
        
        # Configure SSH tunnel settings from parameters or environment variables
        self.ssh_host = ssh_host or os.environ.get(ENV_SSH_HOST)
        self.ssh_port = ssh_port
        if os.environ.get(ENV_SSH_PORT):
            self.ssh_port = int(os.environ.get(ENV_SSH_PORT))
        self.ssh_username = ssh_username or os.environ.get(ENV_SSH_USERNAME)
        self.ssh_password = ssh_password or os.environ.get(ENV_SSH_PASSWORD)
        self.ssh_key_file = ssh_key_file or os.environ.get(ENV_SSH_KEY_FILE)
        self.ssh_key_passphrase = ssh_key_passphrase or os.environ.get(ENV_SSH_KEY_PASSPHRASE)
        self.ssh_local_port = ssh_local_port
        if os.environ.get(ENV_SSH_LOCAL_PORT):
            self.ssh_local_port = int(os.environ.get(ENV_SSH_LOCAL_PORT))
        
        # Initialize SSH tunnel and connection
        self.ssh_tunnel = None
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Trino database with retry mechanism, proxy support, and SSH tunneling"""
        attempt = 0
        last_exception = None
        
        # Setup SSH tunnel if configured
        effective_host = self.host
        effective_port = self.port
        using_tunnel = False
        
        if self.ssh_host:
            try:
                logger.info(f"Setting up SSH tunnel via {self.ssh_host}:{self.ssh_port} to reach {self.host}:{self.port}")
                self.ssh_tunnel = SSHTunnel(
                    ssh_host=self.ssh_host,
                    ssh_port=self.ssh_port,
                    ssh_username=self.ssh_username,
                    ssh_password=self.ssh_password,
                    ssh_key_file=self.ssh_key_file,
                    ssh_key_passphrase=self.ssh_key_passphrase,
                    remote_host=self.host,
                    remote_port=self.port,
                    local_port=self.ssh_local_port
                )
                
                # Start the tunnel and use the local port for connection
                local_port = self.ssh_tunnel.start()
                effective_host = 'localhost'
                effective_port = local_port
                using_tunnel = True
                logger.info(f"Using SSH tunnel - connecting to {effective_host}:{effective_port} to reach {self.host}:{self.port}")
            except Exception as tunnel_error:
                logger.error(f"Failed to establish SSH tunnel: {str(tunnel_error)}")
                if self.ssh_tunnel:
                    self.ssh_tunnel.close()
                    self.ssh_tunnel = None
                # Continue with direct connection attempt
                logger.warning("Falling back to direct connection")
                using_tunnel = False
        
        while attempt < self.max_attempts:
            try:
                # Exponential backoff with jitter
                if attempt > 0:
                    backoff_time = min(60, (2 ** attempt) + random.uniform(0, 1))
                    logger.info(f"Retrying connection in {backoff_time:.2f} seconds (attempt {attempt+1}/{self.max_attempts})")
                    time.sleep(backoff_time)
                
                # Use basic authentication
                auth = BasicAuthentication(self.user, "")
                
                # Set the HTTP headers for client info
                http_headers = {
                    'X-Trino-Client-Info': 'Trino Data Explorer',
                    'X-Trino-Client-Tags': 'streamlit-app'
                }
                
                # Setup proxy configuration for requests
                proxies = {}
                if self.proxy_host and self.proxy_port:
                    proxy_url = f"http://{self.proxy_host}:{self.proxy_port}"
                    proxies = {
                        "http": proxy_url,
                        "https": proxy_url
                    }
                elif self.http_proxy or self.https_proxy:
                    if self.http_proxy:
                        proxies["http"] = self.http_proxy
                    if self.https_proxy:
                        proxies["https"] = self.https_proxy
                
                # Log proxy configuration
                if proxies:
                    logger.info(f"Using proxy configuration: {proxies}")
                
                # Configure connection with timeout and proxy settings
                if proxies:
                    # When using a proxy, we need to adjust the session
                    session = requests.Session()
                    session.proxies = proxies
                    session.verify = False
                    session.trust_env = True
                    
                # Configure connection using the session with proxy
                    self.connection = trino.dbapi.connect(
                        host=effective_host,
                        port=effective_port,
                        user=self.user,
                        catalog=self.catalog,
                        schema=self.schema,
                        http_scheme='https' if not using_tunnel else 'http',
                        verify=False,
                        auth=auth,
                        request_timeout=self.request_timeout,
                        http_headers=http_headers,
                        session=session,
                    )
                else:
                    # Standard connection without proxy
                    self.connection = trino.dbapi.connect(
                        host=effective_host,
                        port=effective_port,
                        user=self.user,
                        catalog=self.catalog,
                        schema=self.schema,
                        http_scheme='https' if not using_tunnel else 'http',
                        verify=False,
                        auth=auth,
                        request_timeout=self.request_timeout,
                        http_headers=http_headers,
                    )
                logger.info(f"Successfully connected to Trino database (catalog: {self.catalog})")
                return  # Connection successful, exit the retry loop
            except urllib3.exceptions.ConnectTimeoutError as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt+1}/{self.max_attempts} timed out: {str(e)}")
                # Additional network troubleshooting information
                if attempt == 0:
                    logger.warning("""
Network connectivity issue detected. This may be because:
1. The Trino server is behind a firewall or in a private network
2. The application is running in a cloud environment (like Streamlit Cloud) that cannot access your private network
3. Network connectivity issues between the application and Trino server

Possible solutions:
1. Set up a proxy server using the TRINO_PROXY_HOST and TRINO_PROXY_PORT environment variables
2. Configure VPN access if your Trino server is in a private network
3. Ensure your Trino server is publicly accessible with proper authentication
4. Use SSH tunneling to connect to your Trino server
5. Deploy your application in the same network as your Trino server
                    """)
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt+1}/{self.max_attempts} failed with network error: {str(e)}")
            except Exception as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt+1}/{self.max_attempts} failed: {str(e)}")
            
            attempt += 1
        
        # If we've exhausted all retries, raise the last exception with detailed error message
        error_msg = f"Failed to connect to Trino after {self.max_attempts} attempts"
        if last_exception:
            error_msg += f": {str(last_exception)}"
            
        # Add deployment environment hint
        error_msg += """
        
NETWORK CONNECTIVITY ISSUE:
If you're deploying on Streamlit Cloud or another cloud environment, you'll need to:
1. Make sure your Trino server is publicly accessible, or
2. Set up a proxy server that can access your private Trino server
3. Configure environment variables for proxy settings:
   - TRINO_PROXY_HOST: Your proxy server hostname
   - TRINO_PROXY_PORT: Your proxy server port
   - HTTP_PROXY/HTTPS_PROXY: Standard proxy environment variables
        """
        
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
        # If we have an SSH tunnel, check if it's still active
        if self.ssh_tunnel and not self.ssh_tunnel.is_active():
            logger.warning("SSH tunnel is no longer active, attempting to reconnect")
            try:
                self._connect()  # This will re-establish the tunnel
            except Exception as e:
                logger.error(f"Failed to re-establish connection: {str(e)}")
                return False
                
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
        """Close the database connection and clean up resources"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.connection = None
        
        # Close SSH tunnel if it exists
        if self.ssh_tunnel:
            try:
                self.ssh_tunnel.close()
                logger.info("SSH tunnel closed successfully")
            except Exception as e:
                logger.error(f"Error closing SSH tunnel: {str(e)}")
            finally:
                self.ssh_tunnel = None
    
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