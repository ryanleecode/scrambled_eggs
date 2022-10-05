use anyhow::Context;
use csv::Trim;
use mysterious_unnamed_rust_project::*;
use std::{
    fs::File,
    io::{self, BufReader, Read},
};

use clap::{Arg, Command};

fn parse_csv(csv: impl Read) -> anyhow::Result<Vec<Transaction>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .from_reader(csv);

    let mut transactions = vec![];
    for result in reader.deserialize() {
        let txn: Transaction = result.with_context(|| "failed to parse transaction")?;
        transactions.push(txn);
    }

    Ok(transactions)
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("MysteriousUnnamedRustProject")
        .arg(Arg::new("transactions_csv_file").required(true))
        .get_matches();
    let transactions_csv_file_path = matches
        .get_one::<String>("transactions_csv_file")
        .expect("csv file path argument to exist");

    let csv_file = File::open(transactions_csv_file_path).with_context(|| {
        format!(
            "csv file: \"{}\" does not exist",
            transactions_csv_file_path
        )
    })?;

    let transactions = parse_csv(BufReader::new(csv_file))
        .with_context(|| "failed to parse transactions from csv file")?;

    let mut client_records = ClientRecords::new();
    for txn in transactions {
        if let Err(err) = client_records.process_transaction(&txn) {
            match err.downcast_ref::<ProcessTransactionError>() {
                Some(_) => {
                    // This is where you would do any complex error logic handling
                    // i.e. log it to a server, send a push notification, etc...
                }
                None => {
                    return Err(err).with_context(|| {
                        format!(
                            "fatal error while processing transaction with id: \"{}\"",
                            txn.tx_id
                        )
                    })
                }
            }
        }
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());
    for client_record in client_records.view().values() {
        wtr.serialize(client_record)?;
    }

    wtr.flush()?;

    Ok(())
}
