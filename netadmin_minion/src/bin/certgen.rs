use std::fs;
use std::fs::File;
use std::io::Write;

use anyhow::{Context, Result};
use rcgen::generate_simple_self_signed;

use netadmin_minion::Minion;

fn main() -> Result<()> {
    let subject_alt_names = vec![Minion::TLS_DOMAIN.to_owned()];
    let cert = generate_simple_self_signed(subject_alt_names).context("Generate certificate")?;

    fs::create_dir_all("__keys")?;
    write("__keys/netadmin_minion.crt", &cert.serialize_pem()?)?;
    write(
        "__keys/netadmin_minion.key",
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
