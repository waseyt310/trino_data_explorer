# Trino Data Explorer

A Streamlit-based web application for exploring and querying Trino databases. This tool allows you to navigate catalogs, schemas, and tables in your Trino database, run custom SQL queries, and view results in an interactive interface.

## Features

- Browse Trino catalogs, schemas, and tables
- View table structure and column information
- Execute custom SQL queries
- Preview table data
- Export results to CSV
- Support for accessing private Trino servers via SSH tunneling or proxies

## Requirements

- Python 3.8+
- Streamlit
- Trino Python client
- Paramiko (for SSH tunneling)
- Other dependencies as specified in `requirements.txt`

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/waseyt310/trino_data_explorer.git
   cd trino_data_explorer
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure your connection (see Configuration section below)

4. Run the application:
   ```
   streamlit run app.py
   ```

## Configuration

The application supports multiple methods for connecting to your Trino server:

### 1. Direct Connection (for publicly accessible Trino servers)

If your Trino server is publicly accessible, you can configure it directly in the `.streamlit/secrets.toml` file:

```toml
[trino]
host = "your-trino-server.example.com"
port = 443
user = "your-username"
catalog = "your-default-catalog"
schema = "your-default-schema"
```

### 2. SSH Tunnel Connection (RECOMMENDED for private Trino servers)

If your Trino server is in a private network, you can use SSH tunneling to securely access it:

```toml
[trino]
host = "private-trino-server.internal"
port = 443
user = "your-username"
catalog = "your-default-catalog"
schema = "your-default-schema"

[ssh]
enabled = true
host = "your-ssh-jump-server.example.com"
port = 22
username = "ssh-username"
password = "ssh-password"  # Or use key-based authentication below
# key_file = "/path/to/private_key"
# key_passphrase = "your-key-passphrase"
```

#### Setting up an SSH jump server

1. Create an SSH server that can access your private Trino server
2. Ensure the SSH server is publicly accessible
3. Set up appropriate authentication (password or SSH keys)
4. Configure the SSH tunnel settings in `.streamlit/secrets.toml`

### 3. HTTP Proxy Connection (Alternative to SSH tunneling)

If you prefer to use an HTTP proxy:

```toml
[trino]
host = "private-trino-server.internal"
port = 443
user = "your-username"
catalog = "your-default-catalog"
schema = "your-default-schema"

[proxy]
host = "your-proxy-server.example.com"
port = 8080
# username = "proxy-username"  # If proxy requires authentication
# password = "proxy-password"
```

### Environment Variables

You can also configure connections using environment variables:

```
# Trino connection
TRINO_HOST=your-trino-server.example.com
TRINO_PORT=443
TRINO_USER=your-username
TRINO_CATALOG=your-default-catalog
TRINO_SCHEMA=your-default-schema

# SSH tunnel configuration
TRINO_SSH_HOST=your-ssh-server.example.com
TRINO_SSH_PORT=22
TRINO_SSH_USERNAME=ssh-username
TRINO_SSH_PASSWORD=ssh-password
TRINO_SSH_KEY_FILE=/path/to/private_key
TRINO_SSH_KEY_PASSPHRASE=your-key-passphrase

# Proxy configuration
TRINO_PROXY_HOST=your-proxy-server.example.com
TRINO_PROXY_PORT=8080
HTTP_PROXY=http://your-proxy-server.example.com:8080
HTTPS_PROXY=http://your-proxy-server.example.com:8080
```

## Deploying to Streamlit Cloud

To deploy this application to Streamlit Cloud:

1. Fork this repository to your GitHub account
2. Log in to [Streamlit Cloud](https://streamlit.io/cloud)
3. Create a new app, pointing to your fork
4. Configure your secrets in the Streamlit Cloud dashboard:
   - Go to App settings > Secrets
   - Add your configuration as shown in the examples above
5. Deploy the app

### Private Trino Server Access from Streamlit Cloud

When deploying to Streamlit Cloud, you'll need special consideration for accessing private Trino servers:

#### Option 1: SSH Tunneling (Recommended)

1. Set up an SSH jump server that:
   - Is publicly accessible from the internet
   - Has access to your private Trino server
   - Allows SSH port forwarding

2. Configure SSH tunnel settings in Streamlit Cloud secrets:
   ```toml
   [trino]
   host = "private-trino-server.internal"
   port = 443
   user = "your-username"
   catalog = "your-default-catalog"
   schema = "your-default-schema"

   [ssh]
   enabled = true
   host = "public-ssh-server.example.com"
   port = 22
   username = "ssh-username"
   password = "ssh-password"
   ```

3. If using key-based authentication (more secure), you'll need to:
   - Generate an SSH key pair
   - Add the private key to your Streamlit secrets
   - Add the public key to your SSH server's authorized_keys file

#### Option 2: Public HTTPS Proxy

1. Set up an HTTPS proxy server that:
   - Is publicly accessible
   - Can access your private Trino server
   - Correctly forwards all Trino API requests

2. Configure the proxy in Streamlit Cloud secrets

## Troubleshooting

### Connection Issues

- **Timeout errors**: If you see connection timeout errors, this typically means:
  - Your Trino server is not accessible from Streamlit Cloud
  - Your SSH tunnel or proxy configuration is incorrect
  - Network connectivity issues exist between components

- **Authentication errors**: Check your username/password or key-based authentication

- **SSH tunnel issues**: 
  - Verify the SSH server is reachable
  - Check that port forwarding is allowed
  - Ensure authentication credentials are correct
  - Confirm the SSH server can reach your Trino server

### Log Files

Check application logs for detailed error messages. In Streamlit Cloud, these can be found in the app's log viewer.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

