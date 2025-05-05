import streamlit as st
import pandas as pd
import time
import io
import base64
from trino_connection import TrinoConnection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set page configuration
st.set_page_config(
    page_title="Trino Data Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Helper functions for error handling
def with_error_handling(func):
    """Decorator for handling function errors gracefully in the UI"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            st.error(f"Error: {error_msg}")
            logger.error(f"Function {func.__name__} failed: {error_msg}")
            return None
    return wrapper

def initialize_connection():
    """Initialize the Trino connection and store in session state"""
    if 'connection' not in st.session_state:
        with st.spinner("Connecting to Trino..."):
            try:
                # Display OAuth2 authentication guidance
                st.info("""
                **OAuth2 Authentication Required**
                
                When prompted, open the authentication URL in your browser to complete the login process.
                You may see 'keyring module not found' messages - this is normal and won't affect functionality.
                """)
                
                # Initialize connection with retry logic
                max_retries = 2
                for attempt in range(max_retries + 1):
                    try:
                        connection = TrinoConnection()
                        if connection.test_connection():
                            # Set all connection related state variables
                            st.session_state.connection = connection
                            st.session_state.connection_status = "Connected"
                            st.session_state.current_catalog = connection.catalog
                            st.session_state.current_schema = connection.schema
                            st.session_state.all_catalogs = connection.get_all_catalogs()
                            
                            # Success message
                            st.success("✅ Successfully connected to Trino")
                            
                            # Also set initial schema and table selections to match current connection
                            st.session_state.selected_schema = connection.schema
                            
                            # Try to get tables for the initial schema
                            try:
                                tables = connection.get_tables(connection.schema)
                                if tables:
                                    st.session_state.selected_table = tables[0]
                                else:
                                    st.session_state.selected_table = ""
                            except Exception as table_error:
                                logger.warning(f"Failed to load initial tables: {str(table_error)}")
                                st.session_state.selected_table = ""
                                
                            return True
                        else:
                            if attempt < max_retries:
                                st.warning(f"Connection attempt {attempt+1}/{max_retries+1} failed. Retrying...")
                                time.sleep(1)  # Short delay before retry
                            else:
                                st.error("Failed to connect to Trino after multiple attempts. Please check your connection settings.")
                                st.session_state.connection_status = "Disconnected"
                                return False
                    except Exception as retry_error:
                        if attempt < max_retries:
                            st.warning(f"Connection attempt {attempt+1}/{max_retries+1} failed: {str(retry_error)}. Retrying...")
                            time.sleep(1)  # Short delay before retry
                        else:
                            raise  # Re-raise the exception on the last attempt
            except Exception as e:
                error_msg = str(e)
                st.error(f"Connection Error: {error_msg}")
                
                # More helpful error message for common issues
                if "OAuth2" in error_msg:
                    st.warning("""
                    **Authentication Issue**
                    
                    OAuth2 authentication failed. Please ensure you:
                    1. Open the authentication URL when prompted
                    2. Complete the login process in your browser
                    3. Return to this app after authentication completes
                    """)
                elif "Connection refused" in error_msg or "timeout" in error_msg.lower():
                    st.warning("""
                    **Network Issue**
                    
                    Could not reach the Trino server. Please check:
                    1. Your network connection
                    2. VPN status (if required)
                    3. Trino server availability
                    """)
                
                st.session_state.connection_status = "Error"
                logger.error(f"Connection initialization failed: {error_msg}")
                return False
    elif 'connection' in st.session_state:
        # Verify existing connection is still valid
        try:
            if not st.session_state.connection.test_connection():
                st.warning("Connection lost. Attempting to reconnect...")
                # Remove existing connection to force reconnection
                del st.session_state.connection
                return initialize_connection()  # Recursive call to reconnect
        except Exception as e:
            st.error(f"Connection verification failed: {str(e)}")
            del st.session_state.connection
            return initialize_connection()  # Recursive call to reconnect
    return True

@with_error_handling
def switch_catalog(catalog, schema=None):
    """Switch to a different catalog and optionally schema"""
    if 'connection' in st.session_state:
        with st.spinner(f"Switching to catalog: {catalog}..."):
            success = st.session_state.connection.switch_catalog(catalog, schema)
            if success:
                st.session_state.current_catalog = catalog
                st.session_state.current_schema = schema if schema else catalog
                st.success(f"Successfully switched to catalog: {catalog}")
                return True
            else:
                st.error(f"Failed to switch to catalog: {catalog}")
                return False
    else:
        st.error("No active connection.")
        return False

@with_error_handling
def get_schemas():
    """Get schemas for the current catalog"""
    if 'connection' in st.session_state:
        query = f"""
        SELECT schema_name 
        FROM {st.session_state.current_catalog}.information_schema.schemata
        """
        df = st.session_state.connection.execute_query(query)
        return df['schema_name'].tolist()
    return []

@with_error_handling
def get_tables(schema=None):
    """Get tables for the specified schema"""
    if 'connection' in st.session_state:
        schema = schema or st.session_state.current_schema
        return st.session_state.connection.get_tables(schema)
    return []

def quote_identifier(identifier):
    """Quote an identifier (table name, schema name, etc.) to handle special characters"""
    if identifier is None:
        return '""'
        
    # Double quote identifiers and escape any internal double quotes
    if '"' in identifier:
        identifier = identifier.replace('"', '""')
    
    # Check for special patterns that might need additional handling
    # For example, if the identifier contains dots outside of quotes which would normally
    # be treated as delimiters in SQL
    if '.' in identifier and not (identifier.startswith('"') and identifier.endswith('"')):
        # If the identifier appears to be a table with a date suffix like "tablename.20190315"
        if any(c.isdigit() for c in identifier.split('.')[-1]):
            # Escape the entire identifier including the dot
            return f'"{identifier}"'
    
    return f'"{identifier}"'

@with_error_handling
def get_table_columns(schema, table):
    """Get column information for a table"""
    if 'connection' in st.session_state:
        # Quote identifiers to handle special characters
        quoted_catalog = quote_identifier(st.session_state.current_catalog)
        quoted_schema = quote_identifier(schema)
        quoted_table = quote_identifier(table)
        
        query = f"""
        SELECT column_name, data_type, is_nullable 
        FROM {quoted_catalog}.information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        try:
            return st.session_state.connection.execute_query(query)
        except Exception as e:
            st.error(f"Error retrieving column information: {str(e)}")
            logger.error(f"Column query failed: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

@with_error_handling
def preview_table(schema, table, limit=100):
    """Get a preview of table data"""
    if 'connection' in st.session_state:
        try:
            # Check for invalid input
            if not schema or not table:
                st.warning("Invalid schema or table name")
                return pd.DataFrame()
                
            # Special handling for table names with date patterns
            is_special_table = False
            original_table = table
            
            # Check if table name contains a date pattern (e.g., table.20190319)
            if '.' in table and any(c.isdigit() for c in table.split('.')[-1]):
                is_special_table = True
                logger.info(f"Special table name detected: {table}")
                # For special tables, we'll take a different approach to quoting

            # Quote identifiers to handle special characters
            quoted_catalog = quote_identifier(st.session_state.current_catalog)
            quoted_schema = quote_identifier(schema)
            
            # Handle regular tables vs special tables differently
            if is_special_table:
                # For special tables with dates, quote the entire table reference
                quoted_table = f'"{table}"'
            else:
                # Regular table quoting
                quoted_table = quote_identifier(table)
            
            # Prepare the query, logging the exact SQL for debugging
            query = f"""
            SELECT * FROM {quoted_catalog}.{quoted_schema}.{quoted_table} LIMIT {limit}
            """
            
            logger.info(f"Executing preview query: {query}")
            
            # Execute query with enhanced error handling
            try:
                result = st.session_state.connection.execute_query(query)
                return result if result is not None else pd.DataFrame()
            except Exception as sql_error:
                error_msg = str(sql_error)
                logger.error(f"SQL error in preview: {error_msg}")
                
                # If we have a syntax error with a special table, try an alternative approach
                if is_special_table and ("syntax error" in error_msg.lower() or "mismatched input" in error_msg.lower()):
                    st.warning("Trying alternative quoting for special table name...")
                    
                    # Try a different quoting approach - enclose the entire reference in quotes
                    alt_query = f"""
                    SELECT * FROM {quoted_catalog}."{schema}.{table}" LIMIT {limit}
                    """
                    
                    logger.info(f"Trying alternative query: {alt_query}")
                    
                    try:
                        result = st.session_state.connection.execute_query(alt_query)
                        if result is not None:
                            st.success("Alternative query approach succeeded!")
                            return result
                    except Exception as alt_error:
                        logger.error(f"Alternative query also failed: {str(alt_error)}")
                
                # Provide helpful error message based on the type of error
                if "syntax error" in error_msg.lower():
                    st.error(f"""
                    **SQL Syntax Error**
                    
                    The table name '{original_table}' contains special characters that caused a syntax error.
                    This typically happens with table names containing dates or special characters.
                    
                    Try using a custom query with different quoting approaches in the "Custom Query" tab.
                    """)
                else:
                    st.error(f"Error retrieving table data: {error_msg}")
                
                return pd.DataFrame()
        except Exception as e:
            st.error(f"Error setting up table preview: {str(e)}")
            logger.error(f"Preview setup failed: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

@with_error_handling
def execute_custom_query(query):
    """Execute a custom SQL query"""
    if 'connection' in st.session_state:
        try:
            result = st.session_state.connection.execute_query(query)
            return result if result is not None else pd.DataFrame()
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            logger.error(f"Custom query failed: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

def get_download_link(df, filename, text):
    """Generate a download link for a dataframe"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">📥 {text}</a>'
    return href

# Main app layout
def main():
    # Initialize all session state variables at the very beginning
    
    # Initialize connection-related state
    if 'connection_status' not in st.session_state:
        st.session_state.connection_status = "Disconnected"
        
    if 'current_catalog' not in st.session_state:
        st.session_state.current_catalog = ""
        
    if 'current_schema' not in st.session_state:
        st.session_state.current_schema = ""
        
    if 'all_catalogs' not in st.session_state:
        st.session_state.all_catalogs = []
    
    # Initialize selection-related state
    if 'selected_schema' not in st.session_state:
        st.session_state.selected_schema = ""
        
    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = ""
        
    # Initialize query-related state
    if 'custom_query' not in st.session_state:
        st.session_state.custom_query = ""
        
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
        
    if 'query_result' not in st.session_state:
        st.session_state.query_result = pd.DataFrame()
        
    if 'preview_data' not in st.session_state:
        st.session_state.preview_data = pd.DataFrame()
            
    # Application title
    st.title("Trino Data Explorer")
    
    # Sidebar for navigation
    st.sidebar.header("Navigation")
    
    # Connection status
    if initialize_connection():
        st.sidebar.success(f"Status: {st.session_state.connection_status}")
        st.sidebar.info(f"Connected to: {st.session_state.current_catalog}.{st.session_state.current_schema}")
        
        # Get available schemas first - we need this for several operations
        schemas = get_schemas()

        # Catalog selection
        st.sidebar.subheader("Catalog Selection")
        catalog_index = 0
        if st.session_state.current_catalog in st.session_state.all_catalogs:
            catalog_index = st.session_state.all_catalogs.index(st.session_state.current_catalog)
            
        selected_catalog = st.sidebar.selectbox(
            "Select Catalog", 
            options=st.session_state.all_catalogs,
            index=catalog_index
        )
        
        # Switch catalog button
        if selected_catalog != st.session_state.current_catalog:
            if st.sidebar.button("Switch Catalog"):
                if switch_catalog(selected_catalog):
                    # Reset schema and table selections for the new catalog
                    schemas = get_schemas()
                    
                    # Update selected schema to current schema or first available
                    if schemas:
                        if st.session_state.current_schema in schemas:
                            st.session_state.selected_schema = st.session_state.current_schema
                        else:
                            st.session_state.selected_schema = schemas[0]
                    else:
                        st.session_state.selected_schema = ""
                    
                    # Reset table selection
                    st.session_state.selected_table = ""
                    
                    # Clean up any preview data or query results
                    st.session_state.preview_data = pd.DataFrame()
                    st.session_state.query_result = pd.DataFrame()
                    
                    # Force a rerun to update the UI with new catalog data
                    st.rerun()
        
        # Schema selection
        st.sidebar.subheader("Schema Selection")
        
        # Always ensure selected_schema has a valid value before proceeding
        if not schemas:
            st.sidebar.warning("No schemas found in this catalog.")
            # Reset schema selection if no schemas are available
            st.session_state.selected_schema = ""
            st.session_state.selected_table = ""
        else:
            # Make sure selected_schema is one of the available schemas
            # If not, default to the first schema
            if not st.session_state.selected_schema or st.session_state.selected_schema not in schemas:
                st.session_state.selected_schema = schemas[0]
                # Also reset the table selection since schema changed
                st.session_state.selected_table = ""
            
            # Create the schema selection dropdown with a fixed key
            schema_index = schemas.index(st.session_state.selected_schema)
            schema_name = st.sidebar.selectbox(
                "Select Schema",
                options=schemas,
                index=schema_index,
                key="schema_selector"  # Use a consistent key
            )
            
            # Update selected_schema if the user selected a different schema
            if schema_name != st.session_state.selected_schema:
                st.session_state.selected_schema = schema_name
                # Reset table selection since schema changed
                st.session_state.selected_table = ""
                # Force a rerun to update the UI
                st.rerun()
            
            # Table selection - only show if a schema is selected
            st.sidebar.subheader("Table Selection")
            
            # Get tables for the selected schema
            tables = get_tables(st.session_state.selected_schema)
            
            if not tables:
                st.sidebar.warning("No tables found in this schema.")
                # Reset table selection if no tables are available
                st.session_state.selected_table = ""
            else:
                # Make sure selected_table is one of the available tables
                # If not, default to the first table
                if not st.session_state.selected_table or st.session_state.selected_table not in tables:
                    st.session_state.selected_table = tables[0]
                
                # Find the correct index for the table in the dropdown
                table_index = tables.index(st.session_state.selected_table)
                
                # Create the table selection dropdown
                table_name = st.sidebar.selectbox(
                    "Select Table",
                    options=tables,
                    index=table_index,
                    key="table_selector"
                )
                
                # Update selected_table if the user selected a different table
                if table_name != st.session_state.selected_table:
                    st.session_state.selected_table = table_name
                    # Force a rerun to update the UI with the new table data
                    st.rerun()
        
        # Main content area
        # This will be expanded in future steps to include table content display,
        # query execution, and data preview
        
        # Placeholder main content
        st.header("Data Explorer")
        st.info("Select a catalog, schema, and table from the sidebar to view data.")
        
        # Display active selections - Only show content when we have valid selections
        if (st.session_state.selected_schema and 
            st.session_state.selected_table and 
            schemas and
            tables and
            st.session_state.selected_schema in schemas and
            st.session_state.selected_table in tables):
            
            # Get current selections from session state
            schema = st.session_state.selected_schema
            table = st.session_state.selected_table
            
            st.subheader(f"Currently Selected: {st.session_state.current_catalog}.{schema}.{table}")
            
            # Create tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs(["Table Structure", "Data Preview", "Custom Query", "Query History"])
            
            # Tab 1: Table Structure
            with tab1:
                st.subheader("Table Columns")
                with st.spinner("Loading column information..."):
                    columns_df = get_table_columns(schema, table)
                    if not columns_df.empty:
                        # Add a column index for easier reference
                        columns_df = columns_df.reset_index(drop=True)
                        columns_df.index += 1
                        
                        # Display column information with highlighting for key fields
                        st.dataframe(
                            columns_df,
                            use_container_width=True,
                            height=400
                        )
                        
                        # Option to download column information
                        st.markdown(
                            get_download_link(
                                columns_df, 
                                f"{table}_columns.csv", 
                                "Download Column Information"
                            ),
                            unsafe_allow_html=True
                        )
                    else:
                        st.warning("No column information available or unable to retrieve column data.")
            
            # Tab 2: Data Preview
            with tab2:
                st.subheader("Table Data Preview")
                
                # Preview settings
                col1, col2 = st.columns([1, 3])
                with col1:
                    preview_limit = st.number_input("Preview Rows", min_value=1, max_value=1000, value=100, step=100)
                with col2:
                    st.markdown("Adjust the number of rows to preview. Larger values may increase loading time.")
                
                # Preview button
                if st.button("Load Preview Data"):
                    with st.spinner(f"Loading preview of {table}..."):
                        try:
                            # Get table preview data with improved error handling
                            preview_df = preview_table(schema, table, preview_limit)
                            
                            # Check if we received valid data
                            if preview_df is not None and hasattr(preview_df, 'empty') and not preview_df.empty:
                                # Store preview data in session state for reuse
                                st.session_state.preview_data = preview_df
                                st.session_state.preview_time = time.time()
                                
                                # Information about the data
                                st.info(f"Showing {len(preview_df)} rows from {schema}.{table}")
                                
                                # Display the data
                                st.dataframe(preview_df, use_container_width=True, height=400)
                                
                                # Option to download preview data
                                st.markdown(
                                    get_download_link(
                                        preview_df, 
                                        f"{table}_preview.csv", 
                                        "Download Preview Data"
                                    ),
                                    unsafe_allow_html=True
                                )
                            else:
                                st.warning("No data available or table is empty.")
                        except Exception as e:
                            st.error(f"Error loading preview data: {str(e)}")
                elif 'preview_data' in st.session_state and hasattr(st.session_state.preview_data, 'empty') and not st.session_state.preview_data.empty:
                    # Display cached preview data if available
                    st.info(f"Showing previously loaded preview data. Click 'Load Preview Data' to refresh.")
                    st.dataframe(st.session_state.preview_data, use_container_width=True, height=400)
                    
                    # Option to download preview data
                    st.markdown(
                        get_download_link(
                            st.session_state.preview_data, 
                            f"{table}_preview.csv", 
                            "Download Preview Data"
                        ),
                        unsafe_allow_html=True
                    )
                else:
                    st.info("Click 'Load Preview Data' to view the table contents.")
            
            # Tab 3: Custom Query
            with tab3:
                st.subheader("Custom SQL Query")
                
                # Query template with quoted identifiers
                quoted_catalog = quote_identifier(st.session_state.current_catalog)
                quoted_schema = quote_identifier(schema)
                quoted_table = quote_identifier(table)
                default_query = f"SELECT * FROM {quoted_catalog}.{quoted_schema}.{quoted_table} LIMIT 100"
                
                # Template buttons
                template_col1, template_col2, template_col3 = st.columns(3)
                with template_col1:
                    if st.button("Select All"):
                        st.session_state.custom_query = default_query
                with template_col2:
                    if st.button("Count Records"):
                        st.session_state.custom_query = f"SELECT COUNT(*) AS record_count FROM {st.session_state.current_catalog}.{schema}.{table}"
                with template_col3:
                    if st.button("Show Schema"):
                        st.session_state.custom_query = f"SELECT column_name, data_type FROM {st.session_state.current_catalog}.information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
                
                # Set default query if needed
                if not st.session_state.custom_query:
                    st.session_state.custom_query = default_query
                
                # Query input - use value instead of key to avoid widget key conflicts
                query = st.text_area("SQL Query", value=st.session_state.custom_query, height=150)
                
                # Update the session state with the new query text
                if query != st.session_state.custom_query:
                    st.session_state.custom_query = query
                
                # Execute query button
                if st.button("Execute Query"):
                    with st.spinner("Executing query..."):
                        try:
                            start_time = time.time()
                            result_df = execute_custom_query(query)
                            execution_time = time.time() - start_time
                            
                            # Check if we received valid data
                            if result_df is not None:
                                # Store query in history
                                if 'query_history' not in st.session_state:
                                    st.session_state.query_history = []
                                
                                # Calculate row count safely
                                row_count = 0
                                if hasattr(result_df, 'empty') and not result_df.empty:
                                    row_count = len(result_df)
                                
                                query_result = {
                                    'query': query,
                                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'execution_time': execution_time,
                                    'row_count': row_count
                                }
                                st.session_state.query_history.insert(0, query_result)
                                
                                # Limit history size
                                if len(st.session_state.query_history) > 10:
                                    st.session_state.query_history = st.session_state.query_history[:10]
                                
                                # Store result in session state
                                st.session_state.query_result = result_df
                                
                                # Display results
                                if hasattr(result_df, 'empty') and not result_df.empty:
                                    st.success(f"Query executed in {execution_time:.2f} seconds | {row_count} rows returned")
                                    st.dataframe(result_df, use_container_width=True, height=400)
                                
                                # Download option
                                st.markdown(
                                    get_download_link(
                                        result_df,
                                        "query_result.csv",
                                        "Download Query Results"
                                    ),
                                    unsafe_allow_html=True
                                )
                            else:
                                st.info("Query executed successfully, but no results were returned.")
                        except Exception as e:
                            st.error(f"Query execution failed: {str(e)}")
                
                # Show previous results if available
                elif 'query_result' in st.session_state:
                    st.info("Showing results from previous query. Click 'Execute Query' to run a new query.")
                    st.dataframe(st.session_state.query_result, use_container_width=True, height=400)
                    
                    # Download option
                    st.markdown(
                        get_download_link(
                            st.session_state.query_result,
                            "query_result.csv",
                            "Download Query Results"
                        ),
                        unsafe_allow_html=True
                    )
            
            # Tab 4: Query History
            with tab4:
                st.subheader("Query History")
                
                if 'query_history' in st.session_state and st.session_state.query_history:
                    for i, query_record in enumerate(st.session_state.query_history):
                        with st.expander(f"Query {i+1} - {query_record['timestamp']} ({query_record['row_count']} rows, {query_record['execution_time']:.2f}s)"):
                            st.code(query_record['query'], language='sql')
                            if st.button(f"Rerun Query {i+1}"):
                                st.session_state.custom_query = query_record['query']
                                st.rerun()
                else:
                    st.info("No query history yet. Execute a query to see it here.")
    else:
        st.sidebar.error("Connection failed. Please check your configuration.")
        st.warning("Unable to connect to Trino. Please verify your connection settings and try again.")

# Run the app
if __name__ == "__main__":
    main()

