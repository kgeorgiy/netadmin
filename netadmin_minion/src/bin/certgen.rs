use std::fs;
use std::fs::File;
use std::io::Write;

use anyhow::{Context, Result};
use rcgen::generate_simple_self_signed;

use netadmin_minion::TlsConfig;

fn main() -> Result<()> {
    let dir = "__keys";

    generate(dir, TlsConfig::MINION_DOMAIN)?;
    generate(dir, TlsConfig::CLIENT_DOMAIN)?;

    Ok(())
}

fn generate(dir: &str, name: &str) -> Result<()> {
    let subject_alt_names = vec![name.to_owned()];
    let cert = generate_simple_self_signed(subject_alt_names).context("Generate certificate")?;

    fs::create_dir_all(dir)?;
    write(&format!("{dir}/{name}.crt"), &cert.serialize_pem()?)?;
    write(
        &format!("{dir}/{name}.key"),
        &cert.serialize_private_key_pem(),
    )?;
    Ok(())
}

fn write(filename: &str, certificate: &str) -> Result<()> {
    let mut result = File::create(filename).context(format!("Create {filename}"))?;
    result
        .write_all(certificate.as_bytes())
        .context(format!("Writing {filename}"))
}
