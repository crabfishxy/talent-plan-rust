use std::{process::exit};
use clap::load_yaml;
use anyhow::Result;

#[macro_use]
extern crate clap;
use clap::App;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yaml");
    let m = App::from_yaml(yaml).get_matches();
    eprint!("unimplemented");
    exit(1);
}
