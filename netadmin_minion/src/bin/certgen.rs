use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::{fs, str};

use anyhow::{anyhow, Context, Result};
use netadmin_minion::Minion;
use rcgen::{Certificate, CertificateParams, DistinguishedName, DnType};

struct CertificateFiles<'a> {
    name: &'a str,
    dir: &'a str,
    pass: &'a str,
    crt: String,
    key: String,
    p12: String,
    jks: String,
}

impl<'a> CertificateFiles<'a> {
    fn new(name: &'a str, dir: &'a str, pass: &'a str) -> CertificateFiles<'a> {
        CertificateFiles {
            name,
            dir,
            pass,
            crt: format!("{dir}/{name}.crt"),
            key: format!("{dir}/{name}.key"),
            p12: format!("{dir}/{name}.p12"),
            jks: format!("{dir}/{name}.jks"),
        }
    }

    fn generate(&self) -> Result<()> {
        let mut d_name = DistinguishedName::new();
        d_name.push(DnType::CommonName, self.name);
        d_name.push(DnType::LocalityName, "St. Petersburg");
        d_name.push(DnType::CountryName, "ru");

        let mut params = CertificateParams::new(vec![self.name.to_owned()]);
        params.distinguished_name = d_name;
        let cert = Certificate::from_params(params).context("Generate certificate")?;

        fs::create_dir_all(self.dir)?;

        write(&self.crt, &cert.serialize_pem()?)?;
        write(&self.key, &cert.serialize_private_key_pem())?;

        run(Command::new("openssl")
            .args(["pkcs12"])
            .args(["-inkey", &self.key])
            .args(["-in", &self.crt])
            .args(["-no-CAfile", "-no-CApath"])
            .args(["-export", "-out", &self.p12])
            .args(["-password", &format!("file:{}", self.pass)]))?;

        if Path::new(&self.jks).exists() {
            fs::remove_file(&self.jks).context("Remove .jks")?;
        }
        run(Command::new("keytool")
            .args(["-importkeystore"])
            .args(["-alias", "1"])
            .args(["-srckeystore", &self.p12])
            .args(["-srcstoretype", "pkcs12"])
            .args(["-srcstorepass:file", self.pass])
            .args(["-destkeystore", &self.jks])
            .args(["-deststoretype", "jks"])
            .args(["-deststorepass:file", self.pass])
            .args(["-destalias:file", self.name]))?;

        Ok(())
    }

    fn add_trust(&self, other: &CertificateFiles) -> Result<()> {
        run(Command::new("keytool")
            .args(["-importcert", "-trustcacerts", "-noprompt"])
            .args(["-alias", other.name])
            .args(["-file", &other.crt])
            .args(["-keystore", &self.jks])
            .args(["-storepass:file", self.pass])
            .args(["-keypass:file", self.pass]))
    }
}

fn main() -> Result<()> {
    let dir = "__keys";
    let pass = "resources/certgen/store.pass";

    let minion = CertificateFiles::new(Minion::MINION_DOMAIN, dir, pass);
    let client = CertificateFiles::new(Minion::CLIENT_DOMAIN, dir, pass);

    minion.generate()?;
    client.generate()?;

    client.add_trust(&minion)?;
    minion.add_trust(&client)?;

    Ok(())
}

#[allow(clippy::print_stdout, clippy::use_debug)]
fn run(command: &mut Command) -> Result<()> {
    println!("Executing {command:?}");
    let output = command.output().context("failed to execute openssl")?;
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

fn write(filename: &str, certificate: &str) -> Result<()> {
    let mut result = File::create(filename).context(format!("Create {filename}"))?;
    result
        .write_all(certificate.as_bytes())
        .context(format!("Writing {filename}"))
}
