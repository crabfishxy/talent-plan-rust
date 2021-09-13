use std::{process::exit};
use clap::load_yaml;
use anyhow::{Result, Error};
use kvs::{KvStore};
use std::env::current_dir;

#[macro_use]
extern crate clap;
use clap::App;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yaml");
    let m = App::from_yaml(yaml).get_matches();
    match m.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("val").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("key").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(Error) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
