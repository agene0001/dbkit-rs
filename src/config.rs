use crate::DbkitError;

/// A supported transactional database backend, detected from the URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    MySql,
    Sqlite,
}

impl Backend {
    /// Detect the backend from a connection URL's scheme
    /// (`postgres://`, `mysql://`, `sqlite://`).
    pub fn from_url(url: &str) -> Result<Self, DbkitError> {
        match url.split(':').next().unwrap_or("") {
            "postgres" | "postgresql" => Ok(Backend::Postgres),
            "mysql" => Ok(Backend::MySql),
            "sqlite" => Ok(Backend::Sqlite),
            other => Err(DbkitError::UnsupportedBackend(other.to_string())),
        }
    }

    /// The URL scheme used when building a URL from parts.
    fn scheme(self) -> &'static str {
        match self {
            Backend::Postgres => "postgres",
            Backend::MySql => "mysql",
            Backend::Sqlite => "sqlite",
        }
    }

    /// The default TCP port for server backends (0 for SQLite, which is fileless).
    fn default_port(self) -> u16 {
        match self {
            Backend::Postgres => 5432,
            Backend::MySql => 3306,
            Backend::Sqlite => 0,
        }
    }
}

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
    /// Connection URL. The scheme selects the backend
    /// (`postgres://`, `mysql://`, `sqlite://`).
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
    backend: Backend,
    host: String,
    port: Option<u16>,
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
            backend: Backend::Postgres,
            host: "localhost".into(),
            port: None,
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
    /// Select the target backend. Defaults to [`Backend::Postgres`].
    pub fn backend(mut self, backend: Backend) -> Self {
        self.backend = backend;
        self
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
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
    ///
    /// For [`Backend::Sqlite`] the URL is `sqlite://{database}`, treating
    /// `database` as a file path; host/port/auth/SSL are ignored. SQLite users
    /// are usually better served by [`DbkitConfig::from_url`].
    pub fn build(self) -> DbkitConfig {
        let url = match self.backend {
            Backend::Sqlite => format!("sqlite://{}", self.database),
            backend => {
                let auth = match (&self.user, &self.password) {
                    (Some(u), Some(p)) => format!("{}:{}@", u, p),
                    (Some(u), None) => format!("{}@", u),
                    _ => String::new(),
                };

                let port = self.port.unwrap_or_else(|| backend.default_port());

                let ssl_param = match self.ssl_mode {
                    SslMode::Disable => "",
                    SslMode::Prefer => "?sslmode=prefer",
                    SslMode::Require => "?sslmode=require",
                };

                format!(
                    "{}://{}{}:{}/{}{}",
                    backend.scheme(),
                    auth,
                    self.host,
                    port,
                    self.database,
                    ssl_param
                )
            }
        };

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

    #[test]
    fn test_builder_mysql_default_port() {
        let config = DbkitConfig::builder()
            .backend(Backend::MySql)
            .user("root")
            .database("app")
            .build();
        assert_eq!(config.url, "mysql://root@localhost:3306/app");
    }

    #[test]
    fn test_builder_sqlite() {
        let config = DbkitConfig::builder()
            .backend(Backend::Sqlite)
            .database("data/app.db")
            .build();
        assert_eq!(config.url, "sqlite://data/app.db");
    }

    #[test]
    fn test_backend_from_url() {
        assert_eq!(Backend::from_url("postgres://x/y").unwrap(), Backend::Postgres);
        assert_eq!(Backend::from_url("postgresql://x/y").unwrap(), Backend::Postgres);
        assert_eq!(Backend::from_url("mysql://x/y").unwrap(), Backend::MySql);
        assert_eq!(Backend::from_url("sqlite://f.db").unwrap(), Backend::Sqlite);
        assert!(Backend::from_url("oracle://x/y").is_err());
    }
}
