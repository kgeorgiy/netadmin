use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    str,
};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use netadmin_minion::{log::Log, Minion};
use rcgen::{Certificate, CertificateParams, DistinguishedName, DnType};
use serde::{Deserialize, Serialize};
use tracing::info;

struct CertificateFiles {
    name: String,
    dir: PathBuf,
    pass: String,
    crt: String,
    key: String,
    p12: String,
    jks: String,
}

impl CertificateFiles {
    fn new(name: &str, target: &Path, pass: &str) -> Result<CertificateFiles> {
        let dir = target
            .parent()
            .context(format!("No parent directory for {target:?}"))?;
        Ok(CertificateFiles {
            name: name.to_owned(),
            dir: dir.to_owned(),
            pass: pass.to_owned(),
            crt: Self::with_extension(target, "crt"),
            key: Self::with_extension(target, "key"),
            p12: Self::with_extension(target, "p12"),
            jks: Self::with_extension(target, "jks"),
        })
    }

    fn with_extension(target: &Path, extension: &str) -> String {
        target
            .with_extension(extension)
            .to_str()
            .expect("unicode")
            .to_owned()
    }

    fn generate(&self) -> Result<()> {
        let mut d_name = DistinguishedName::new();
        d_name.push(DnType::CommonName, &self.name);
        d_name.push(DnType::LocalityName, "St. Petersburg");
        d_name.push(DnType::CountryName, "ru");

        let mut params = CertificateParams::new(vec![self.name.clone()]);
        params.distinguished_name = d_name;
        let cert = Certificate::from_params(params).context("Generate certificate")?;

        fs::create_dir_all(&self.dir)?;

        Self::write(&self.crt, &cert.serialize_pem()?)?;
        Self::write(&self.key, &cert.serialize_private_key_pem())?;

        Ok(())
    }

    fn generate_legacy(&self) -> Result<()> {
        Self::run(
            Command::new("openssl")
                .args(["pkcs12"])
                .args(["-inkey", &self.key])
                .args(["-in", &self.crt])
                .args(["-no-CAfile", "-no-CApath"])
                .args(["-export", "-out", &self.p12])
                .args(["-password", &format!("pass:{}", self.pass)]),
        )?;

        if Path::new(&self.jks).exists() {
            fs::remove_file(&self.jks).context("Remove .jks")?;
        }
        Self::run(
            Command::new("keytool")
                .args(["-importkeystore"])
                .args(["-alias", "1"])
                .args(["-srckeystore", &self.p12])
                .args(["-srcstoretype", "pkcs12"])
                .args(["-srcstorepass", &self.pass])
                .args(["-destkeystore", &self.jks])
                .args(["-deststoretype", "jks"])
                .args(["-deststorepass", &self.pass])
                .args(["-destalias", &self.name]),
        )?;
        Ok(())
    }

    fn write(filename: &str, certificate: &str) -> Result<()> {
        info!("Writing {filename}");
        File::create(filename)
            .context(format!("Create {filename}"))?
            .write_all(certificate.as_bytes())
            .context(format!("Writing {filename}"))
    }

    fn add_trust(&self, other: &CertificateFiles) -> Result<()> {
        Self::run(
            Command::new("keytool")
                .args(["-importcert", "-trustcacerts", "-noprompt"])
                .args(["-alias", &other.name])
                .args(["-file", &other.crt])
                .args(["-keystore", &self.jks])
                .args(["-storepass", &self.pass])
                .args(["-keypass", &self.pass]),
        )
    }

    fn run(command: &mut Command) -> Result<()> {
        info!("Executing {command:?}");
        let output = command.output()
            .context(format!("failed to execute {:?}", command.get_program()))?;
        let exit_code = output.status.code().unwrap_or(-1);
        if exit_code == 0 {
            Ok(())
        } else {
            Err(anyhow!(format!(
                "Exit code {}\n{}\n{}",
                output.status,
                str::from_utf8(&output.stderr)?,
                str::from_utf8(&output.stdout)?,
            )))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CertgenConfig {
    minion: CertificateConfig,
    server: CertificateConfig,
    legacy: LegacyConfig,
}

impl CertgenConfig {
    fn generate(&self, legacy: bool) -> Result<()> {
        let minion = self.minion.files(&self.legacy.keystore_password)?;
        let server = self.server.files(&self.legacy.keystore_password)?;

        minion.generate()?;
        server.generate()?;

        if legacy {
            minion.generate_legacy()?;
            server.generate_legacy()?;
            server.add_trust(&minion)?;
            minion.add_trust(&server)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CertificateConfig {
    domain: Option<String>,
    target: PathBuf,
}

impl CertificateConfig {
    fn resolve(&mut self, base: &Path, domain: &str) {
        self.target = base.join(&self.target);
        self.domain = Some(
            self.domain
                .take()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| domain.to_owned()),
        );
    }

    fn files(&self, pass: &str) -> Result<CertificateFiles> {
        CertificateFiles::new(
            self.domain.as_ref().context("resolved")?,
            &self.target,
            pass,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyConfig {
    #[serde(alias = "keystore-password")]
    keystore_password: String,
}

#[derive(Parser)]
#[command(author, version, about, long_about = "NetAdmin certificate generator")]
struct Cli {
    /// Configuration file location
    #[clap(
        short,
        long,
        value_name = "FILE",
        default_value = "resources/netadmin-certgen.yaml"
    )]
    config: PathBuf,

    /// Generate `.c12` and `.jks` files
    #[clap(long, default_value_t = true)]
    legacy: bool,

    /// Do not generate `.c12` and `.jks` files
    #[clap(long, default_value_t = false)]
    no_legacy: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let _log = Log::local();

    let result = load_config(&cli.config)?.generate(cli.legacy && !cli.no_legacy);
    Log::log_result(&result);
    result
}

fn load_config(path: &Path) -> Result<CertgenConfig> {
    let mut config: CertgenConfig = Log::load_config(path)?;
    let base = path.parent().context("Has parent path")?;
    config.minion.resolve(base, Minion::MINION_DOMAIN);
    config.server.resolve(base, Minion::CLIENT_DOMAIN);
    Ok(config)
}
