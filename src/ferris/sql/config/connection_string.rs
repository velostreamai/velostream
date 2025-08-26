//! Connection String URI Parser
//!
//! This module provides comprehensive URI parsing for data source connection strings.
//! It supports standard URI formats with scheme-specific extensions for different data sources.
//!
//! ## Supported URI Formats
//!
//! - **Kafka**: `kafka://broker1:9092,broker2:9093/topic?group_id=analytics&auto_commit=false`
//! - **S3**: `s3://bucket/prefix/*.parquet?region=us-west-2&access_key=key`
//! - **File**: `file:///path/to/data.json?watch=true&format=json`
//! - **PostgreSQL**: `postgresql://user:pass@host:5432/database?sslmode=require`
//! - **ClickHouse**: `clickhouse://user:pass@host:9000/database?compression=lz4`
//! - **HTTP**: `http://api.example.com/data?auth=token&format=json`
//!
//! ## Advanced Features
//!
//! - **Multi-host Support**: Comma-separated hosts for clusters
//! - **Path Patterns**: Wildcards and glob patterns for file systems
//! - **Query Parameters**: Full query string parsing with type conversion
//! - **Validation**: Scheme-specific validation rules
//! - **Normalization**: Automatic URI normalization

use crate::ferris::sql::config::{ConfigSource, DataSourceConfig};
use std::collections::HashMap;
use std::fmt;

/// Parsed connection string components
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionString {
    /// URI scheme (kafka, s3, file, etc.)
    pub scheme: String,

    /// User information (username:password)
    pub userinfo: Option<String>,

    /// Host addresses (supports multiple hosts for clusters)
    pub hosts: Vec<HostPort>,

    /// Path component (topic, file path, S3 prefix, etc.)
    pub path: Option<String>,

    /// Query parameters parsed into key-value pairs
    pub query: HashMap<String, String>,

    /// Fragment identifier
    pub fragment: Option<String>,

    /// Original URI string
    pub original: String,
}

/// Host and port combination
#[derive(Debug, Clone, PartialEq)]
pub struct HostPort {
    /// Host address or name
    pub host: String,

    /// Port number (optional)
    pub port: Option<u16>,
}

/// URI components for structured access
#[derive(Debug, Clone, PartialEq)]
pub struct UriComponents {
    /// Connection string
    pub connection: ConnectionString,

    /// Primary host (first in list)
    pub primary_host: Option<HostPort>,

    /// All hosts as formatted strings
    pub host_strings: Vec<String>,

    /// Username extracted from userinfo
    pub username: Option<String>,

    /// Password extracted from userinfo
    pub password: Option<String>,
}

impl ConnectionString {
    /// Parse a URI string into connection components
    ///
    /// # Arguments
    /// * `uri` - The URI string to parse
    ///
    /// # Returns
    /// * `Ok(ConnectionString)` - Successfully parsed connection
    /// * `Err(ParseError)` - Parsing failed with detailed error
    ///
    /// # Examples
    /// ```rust
    /// use ferrisstreams::ferris::sql::config::connection_string::ConnectionString;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = ConnectionString::parse("kafka://localhost:9092/orders?group_id=analytics")?;
    /// assert_eq!(conn.scheme, "kafka");
    /// assert_eq!(conn.hosts[0].host, "localhost");
    /// assert_eq!(conn.hosts[0].port, Some(9092));
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse(uri: &str) -> Result<Self, ParseError> {
        if uri.is_empty() {
            return Err(ParseError::EmptyUri);
        }

        let original = uri.to_string();
        let mut remaining = uri;

        // Parse scheme
        let scheme = if let Some(scheme_end) = remaining.find("://") {
            let scheme = remaining[..scheme_end].to_lowercase();
            if scheme.is_empty() {
                return Err(ParseError::MissingScheme);
            }
            remaining = &remaining[scheme_end + 3..];
            scheme
        } else {
            return Err(ParseError::MissingScheme);
        };

        // Parse fragment (if present at the end)
        let fragment = if let Some(fragment_start) = remaining.rfind('#') {
            let fragment = remaining[fragment_start + 1..].to_string();
            remaining = &remaining[..fragment_start];
            Some(fragment).filter(|f| !f.is_empty())
        } else {
            None
        };

        // Parse query parameters (if present)
        let query = if let Some(query_start) = remaining.find('?') {
            let query_str = &remaining[query_start + 1..];
            remaining = &remaining[..query_start];
            Self::parse_query(query_str)?
        } else {
            HashMap::new()
        };

        // Parse path (if present)
        let path = if let Some(path_start) = remaining.find('/') {
            let path = remaining[path_start..].to_string();
            remaining = &remaining[..path_start];
            Some(path).filter(|p| !p.is_empty() && p != "/")
        } else {
            None
        };

        // Parse userinfo and authority
        let (userinfo, hosts) = if remaining.contains('@') {
            let parts: Vec<&str> = remaining.rsplitn(2, '@').collect();
            if parts.len() != 2 {
                return Err(ParseError::InvalidUserInfo);
            }

            let userinfo = if parts[1].is_empty() {
                None
            } else {
                Some(parts[1].to_string())
            };

            let hosts = Self::parse_hosts(parts[0], &scheme)?;
            (userinfo, hosts)
        } else {
            let hosts = Self::parse_hosts(remaining, &scheme)?;
            (None, hosts)
        };

        Ok(ConnectionString {
            scheme,
            userinfo,
            hosts,
            path,
            query,
            fragment,
            original,
        })
    }

    /// Parse multiple hosts (cluster support)
    fn parse_hosts(host_str: &str, scheme: &str) -> Result<Vec<HostPort>, ParseError> {
        if host_str.is_empty() {
            // Some schemes allow empty hosts (like file://)
            if matches!(scheme, "file") {
                return Ok(vec![]);
            } else {
                return Err(ParseError::MissingHost);
            }
        }

        let mut hosts = Vec::new();

        // Split by comma for multiple hosts (cluster support)
        for host_part in host_str.split(',') {
            let host_part = host_part.trim();
            if host_part.is_empty() {
                continue;
            }

            let host_port = if host_part.contains(':') {
                let parts: Vec<&str> = host_part.rsplitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(ParseError::InvalidHostPort(host_part.to_string()));
                }

                let port_str = parts[0];
                let host = parts[1].to_string();

                let port = port_str
                    .parse::<u16>()
                    .map_err(|_| ParseError::InvalidPort(port_str.to_string()))?;

                HostPort {
                    host,
                    port: Some(port),
                }
            } else {
                HostPort {
                    host: host_part.to_string(),
                    port: None,
                }
            };

            hosts.push(host_port);
        }

        if hosts.is_empty() && !matches!(scheme, "file") {
            return Err(ParseError::MissingHost);
        }

        Ok(hosts)
    }

    /// Parse query parameters
    fn parse_query(query_str: &str) -> Result<HashMap<String, String>, ParseError> {
        let mut query = HashMap::new();

        if query_str.is_empty() {
            return Ok(query);
        }

        for param in query_str.split('&') {
            if param.is_empty() {
                continue;
            }

            let (key, value) = if let Some(eq_pos) = param.find('=') {
                let key = &param[..eq_pos];
                let value = &param[eq_pos + 1..];
                (key, value)
            } else {
                (param, "")
            };

            let key = Self::url_decode(key)
                .map_err(|_| ParseError::InvalidQueryParameter(param.to_string()))?;
            let value = Self::url_decode(value)
                .map_err(|_| ParseError::InvalidQueryParameter(param.to_string()))?;

            query.insert(key, value);
        }

        Ok(query)
    }

    /// Simple URL decoding for query parameters
    fn url_decode(s: &str) -> Result<String, ()> {
        let mut result = String::new();
        let mut chars = s.chars();

        while let Some(ch) = chars.next() {
            if ch == '%' {
                let hex1 = chars.next().ok_or(())?;
                let hex2 = chars.next().ok_or(())?;
                let hex_str = format!("{}{}", hex1, hex2);
                let byte = u8::from_str_radix(&hex_str, 16).map_err(|_| ())?;
                result.push(byte as char);
            } else if ch == '+' {
                result.push(' ');
            } else {
                result.push(ch);
            }
        }

        Ok(result)
    }

    /// Get components with extracted user info
    pub fn components(&self) -> UriComponents {
        let primary_host = self.hosts.first().cloned();

        let host_strings: Vec<String> = self
            .hosts
            .iter()
            .map(|hp| {
                if let Some(port) = hp.port {
                    format!("{}:{}", hp.host, port)
                } else {
                    hp.host.clone()
                }
            })
            .collect();

        let (username, password) = if let Some(ref userinfo) = self.userinfo {
            if let Some(colon_pos) = userinfo.find(':') {
                let username = userinfo[..colon_pos].to_string();
                let password = userinfo[colon_pos + 1..].to_string();
                (Some(username), Some(password))
            } else {
                (Some(userinfo.clone()), None)
            }
        } else {
            (None, None)
        };

        UriComponents {
            connection: self.clone(),
            primary_host,
            host_strings,
            username,
            password,
        }
    }

    /// Validate the connection string for a specific scheme
    pub fn validate(&self) -> Result<(), ParseError> {
        match self.scheme.as_str() {
            "kafka" => self.validate_kafka(),
            "s3" => self.validate_s3(),
            "file" => self.validate_file(),
            "postgresql" | "postgres" => self.validate_postgresql(),
            "clickhouse" => self.validate_clickhouse(),
            "http" | "https" => self.validate_http(),
            _ => Ok(()), // Unknown schemes are allowed but not validated
        }
    }

    fn validate_kafka(&self) -> Result<(), ParseError> {
        if self.hosts.is_empty() {
            return Err(ParseError::SchemeValidationError {
                scheme: "kafka".to_string(),
                message: "At least one broker host is required".to_string(),
            });
        }

        for host_port in &self.hosts {
            if host_port.host.is_empty() {
                return Err(ParseError::SchemeValidationError {
                    scheme: "kafka".to_string(),
                    message: "Host cannot be empty".to_string(),
                });
            }
        }

        Ok(())
    }

    fn validate_s3(&self) -> Result<(), ParseError> {
        if self.hosts.is_empty() || self.hosts[0].host.is_empty() {
            return Err(ParseError::SchemeValidationError {
                scheme: "s3".to_string(),
                message: "S3 bucket name is required".to_string(),
            });
        }

        Ok(())
    }

    fn validate_file(&self) -> Result<(), ParseError> {
        if self.path.is_none() {
            return Err(ParseError::SchemeValidationError {
                scheme: "file".to_string(),
                message: "File path is required".to_string(),
            });
        }

        Ok(())
    }

    fn validate_postgresql(&self) -> Result<(), ParseError> {
        if self.hosts.is_empty() {
            return Err(ParseError::SchemeValidationError {
                scheme: "postgresql".to_string(),
                message: "Database host is required".to_string(),
            });
        }

        Ok(())
    }

    fn validate_clickhouse(&self) -> Result<(), ParseError> {
        if self.hosts.is_empty() {
            return Err(ParseError::SchemeValidationError {
                scheme: "clickhouse".to_string(),
                message: "ClickHouse host is required".to_string(),
            });
        }

        Ok(())
    }

    fn validate_http(&self) -> Result<(), ParseError> {
        if self.hosts.is_empty() {
            return Err(ParseError::SchemeValidationError {
                scheme: self.scheme.clone(),
                message: "HTTP host is required".to_string(),
            });
        }

        Ok(())
    }

    /// Reconstruct the URI string
    pub fn to_uri(&self) -> String {
        let mut uri = format!("{}://", self.scheme);

        if let Some(ref userinfo) = self.userinfo {
            uri.push_str(userinfo);
            uri.push('@');
        }

        if !self.hosts.is_empty() {
            let host_strings: Vec<String> = self
                .hosts
                .iter()
                .map(|hp| {
                    if let Some(port) = hp.port {
                        format!("{}:{}", hp.host, port)
                    } else {
                        hp.host.clone()
                    }
                })
                .collect();
            uri.push_str(&host_strings.join(","));
        }

        if let Some(ref path) = self.path {
            if !path.starts_with('/') {
                uri.push('/');
            }
            uri.push_str(path);
        }

        if !self.query.is_empty() {
            uri.push('?');
            let params: Vec<String> = self
                .query
                .iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        k.clone()
                    } else {
                        format!("{}={}", k, v)
                    }
                })
                .collect();
            uri.push_str(&params.join("&"));
        }

        if let Some(ref fragment) = self.fragment {
            uri.push('#');
            uri.push_str(fragment);
        }

        uri
    }
}

impl From<ConnectionString> for DataSourceConfig {
    fn from(conn: ConnectionString) -> Self {
        let mut config = DataSourceConfig::new(&conn.scheme);

        if let Some(primary_host) = conn.hosts.first() {
            config.host = Some(primary_host.host.clone());
            config.port = primary_host.port;
        }

        config.path = conn.path;
        config.parameters = conn.query;
        config.source = ConfigSource::Uri(conn.original.clone());

        config
    }
}

impl fmt::Display for ConnectionString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uri())
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(port) = self.port {
            write!(f, "{}:{}", self.host, port)
        } else {
            write!(f, "{}", self.host)
        }
    }
}

/// URI parsing errors
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Empty URI string
    EmptyUri,

    /// Missing URI scheme
    MissingScheme,

    /// Missing host in URI
    MissingHost,

    /// Invalid host:port format
    InvalidHostPort(String),

    /// Invalid port number
    InvalidPort(String),

    /// Invalid user information format
    InvalidUserInfo,

    /// Invalid query parameter
    InvalidQueryParameter(String),

    /// Scheme-specific validation error
    SchemeValidationError { scheme: String, message: String },

    /// Unsupported URI format
    UnsupportedFormat(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EmptyUri => write!(f, "URI cannot be empty"),
            ParseError::MissingScheme => {
                write!(f, "URI must include a scheme (e.g., kafka://, s3://)")
            }
            ParseError::MissingHost => write!(f, "URI must include a host"),
            ParseError::InvalidHostPort(hp) => write!(f, "Invalid host:port format: '{}'", hp),
            ParseError::InvalidPort(port) => write!(f, "Invalid port number: '{}'", port),
            ParseError::InvalidUserInfo => write!(f, "Invalid user information format"),
            ParseError::InvalidQueryParameter(param) => {
                write!(f, "Invalid query parameter: '{}'", param)
            }
            ParseError::SchemeValidationError { scheme, message } => {
                write!(f, "Validation error for scheme '{}': {}", scheme, message)
            }
            ParseError::UnsupportedFormat(format) => {
                write!(f, "Unsupported URI format: '{}'", format)
            }
        }
    }
}

impl std::error::Error for ParseError {}
