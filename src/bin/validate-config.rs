use st0x_issuance::Config;

fn main() -> anyhow::Result<()> {
    Config::parse()?;
    println!("configuration valid");
    Ok(())
}
