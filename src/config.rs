/// Configuration for a dbkit database connection.
///
/// Can be built from a URL string or constructed with the builder.
///
/// # Example
/// ```
/// use dbkit::DbkitConfig;
///
/// // From URL
/// let config = DbkitConfig::from_url("postgres://localhost/mydb");
///
/// // From builder
/// let config = DbkitConfig::builder()
///     .host("db.example.com")
///     .port(5432)
///     .database("myapp")
///     .user("admin")
///     .password("secret")
///     .pool_size(16)
///     .connect_timeout_secs(10)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct DbkitConfig {
    /// Postgres connection URL.
    pub url: String,
    /// Maximum pool size. Default: 16.
    pub pool_size: usize,
    /// Connection timeout in seconds. Default: 30.
    pub connect_timeout_secs: u64,
    /// Idle timeout in seconds. Connections idle longer are reaped. Default: 300.
    pub idle_timeout_secs: u64,
    /// Auto-create the database if it doesn't exist. Default: true.
    pub auto_create_db: bool,
}

impl DbkitConfig {
    /// Create config from a connection URL with default settings.
    pub fn from_url(url: &str) -> Self {
        Self {
            url: url.to_string(),
            pool_size: 16,
            connect_timeout_secs: 30,
            idle_timeout_secs: 300,
            auto_create_db: true,
        }
    }

    /// Start building a config from individual connection parameters.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

/// Builder for constructing a [`DbkitConfig`] from individual parameters.
pub struct ConfigBuilder {
    host: String,
    port: u16,
    database: String,
    user: Option<String>,
    password: Option<String>,
    pool_size: usize,
    connect_timeout_secs: u64,
    idle_timeout_secs: u64,
    auto_create_db: bool,
    ssl_mode: SslMode,
}

/// SSL mode for Postgres connections.
#[derive(Debug, Clone, Copy, Default)]
pub enum SslMode {
    /// No SSL (default — matches current NoTls behavior).
    #[default]
    Disable,
    /// Prefer SSL but allow fallback.
    Prefer,
    /// Require SSL.
    Require,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            database: "postgres".into(),
            user: None,
            password: None,
            pool_size: 16,
            connect_timeout_secs: 30,
            idle_timeout_secs: 300,
            auto_create_db: true,
            ssl_mode: SslMode::default(),
        }
    }
}

impl ConfigBuilder {
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }

    pub fn user(mut self, user: &str) -> Self {
        self.user = Some(user.to_string());
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    pub fn pool_size(mut self, size: usize) -> Self {
        self.pool_size = size;
        self
    }

    pub fn connect_timeout_secs(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }

    pub fn idle_timeout_secs(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = secs;
        self
    }

    pub fn auto_create_db(mut self, enabled: bool) -> Self {
        self.auto_create_db = enabled;
        self
    }

    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Build the config, constructing the connection URL from parts.
    pub fn build(self) -> DbkitConfig {
        let auth = match (&self.user, &self.password) {
            (Some(u), Some(p)) => format!("{}:{}@", u, p),
            (Some(u), None) => format!("{}@", u),
            _ => String::new(),
        };

        let ssl_param = match self.ssl_mode {
            SslMode::Disable => "",
            SslMode::Prefer => "?sslmode=prefer",
            SslMode::Require => "?sslmode=require",
        };

        let url = format!(
            "postgres://{}{}:{}/{}{}",
            auth, self.host, self.port, self.database, ssl_param
        );

        DbkitConfig {
            url,
            pool_size: self.pool_size,
            connect_timeout_secs: self.connect_timeout_secs,
            idle_timeout_secs: self.idle_timeout_secs,
            auto_create_db: self.auto_create_db,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_url() {
        let config = DbkitConfig::from_url("postgres://localhost/mydb");
        assert_eq!(config.url, "postgres://localhost/mydb");
        assert_eq!(config.pool_size, 16);
        assert!(config.auto_create_db);
    }

    #[test]
    fn test_builder_full() {
        let config = DbkitConfig::builder()
            .host("db.example.com")
            .port(5433)
            .database("myapp")
            .user("admin")
            .password("secret")
            .pool_size(32)
            .connect_timeout_secs(10)
            .ssl_mode(SslMode::Require)
            .build();

        assert_eq!(
            config.url,
            "postgres://admin:secret@db.example.com:5433/myapp?sslmode=require"
        );
        assert_eq!(config.pool_size, 32);
        assert_eq!(config.connect_timeout_secs, 10);
    }

    #[test]
    fn test_builder_minimal() {
        let config = DbkitConfig::builder().database("test").build();
        assert_eq!(config.url, "postgres://localhost:5432/test");
    }

    #[test]
    fn test_builder_user_no_password() {
        let config = DbkitConfig::builder()
            .user("readonly")
            .database("prod")
            .build();
        assert_eq!(config.url, "postgres://readonly@localhost:5432/prod");
    }
}
