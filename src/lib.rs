use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use serde::{ser::SerializeStruct, Serialize};
use thiserror::Error;

mod tx;

pub use tx::*;

#[derive(Error, Debug, PartialEq)]
pub enum ProcessTransactionError {
    #[error("transaction: \"{0}\" has already been processed")]
    DuplicateTransaction(u32),
    #[error("${1} transaction: \"{0}\" failed. client has insufficient funds")]
    InsufficientFunds(u32, TransactionType),
    #[error(
        "cannot ${2} transaction: \"{0}\" with client id: \"{1}\". no deposit with this id exists"
    )]
    MissingTransaction(u32, u16, TransactionType),
    #[error("${1} transaction: \"{0}\" failed. last transaction state was: {2}")]
    InvalidTransactionState(u32, TransactionType, TransactionType),
    #[error("${1} transaction: \"{0}\" failed. client account: {1} is frozen.")]
    ClientAccountFrozen(u32, TransactionType, u16),
}

#[derive(Debug, PartialEq)]
pub struct Client {
    client_id: u16,
    available_amounts: f32,
    held_amounts: f32,
    is_locked: bool,
}

impl Serialize for Client {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Client", 5)?;
        state.serialize_field("client", &self.client_id)?;
        state.serialize_field("available", &format!("{:.4}", &self.available_amounts))?;
        state.serialize_field("held", &format!("{:.4}", &self.held_amounts))?;
        state.serialize_field("total", &format!("{:.4}", &self.total_amounts()))?;
        state.serialize_field("locked", &self.is_locked)?;
        state.end()
    }
}

impl Client {
    pub fn new(client_id: u16) -> Client {
        return Client {
            client_id,
            available_amounts: 0.0,
            held_amounts: 0.0,
            is_locked: false,
        };
    }

    fn total_amounts(&self) -> f32 {
        return self.held_amounts + self.available_amounts;
    }
}

#[derive(Debug)]
pub struct ClientRecords {
    records: HashMap<u16, Client>,
    deposits: HashMap<u32, Deposit>,
    withdrawals: HashSet<u32>,
}

impl ClientRecords {
    pub fn new() -> ClientRecords {
        return ClientRecords {
            records: HashMap::new(),
            deposits: HashMap::new(),
            withdrawals: HashSet::new(),
        };
    }

    pub fn view(&self) -> &HashMap<u16, Client> {
        return &self.records;
    }

    fn is_txn_processed(&self, id: u32) -> bool {
        return self.deposits.contains_key(&id) || self.withdrawals.contains(&id);
    }

    pub fn process_transaction(&mut self, txn: &Transaction) -> anyhow::Result<()> {
        let is_txn_processed = self.is_txn_processed(txn.tx_id);
        let record = self
            .records
            .entry(txn.client_id)
            .or_insert_with(|| Client::new(txn.client_id));
        let amount = txn.amount.unwrap_or(0.0);

        match txn.txn_type {
            TransactionType::Deposit => {
                if is_txn_processed {
                    return Err(anyhow!(ProcessTransactionError::DuplicateTransaction(
                        txn.tx_id
                    )));
                }

                record.available_amounts += amount;
                self.deposits.insert(
                    txn.tx_id,
                    Deposit {
                        client_id: txn.client_id,
                        amount,
                        status: TransactionType::Deposit,
                    },
                );
            }
            TransactionType::Withdrawal => {
                if is_txn_processed {
                    return Err(anyhow!(ProcessTransactionError::DuplicateTransaction(
                        txn.tx_id
                    )));
                }

                if record.is_locked {
                    return Err(anyhow!(ProcessTransactionError::ClientAccountFrozen(
                        txn.tx_id,
                        txn.txn_type,
                        txn.client_id
                    )));
                }

                if record.available_amounts < amount {
                    return Err(anyhow!(ProcessTransactionError::InsufficientFunds(
                        txn.tx_id,
                        txn.txn_type,
                    )));
                }

                record.available_amounts -= amount;
                self.withdrawals.insert(txn.tx_id);
            }
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                if let Some(Deposit {
                    status,
                    client_id,
                    amount,
                }) = self.deposits.get_mut(&txn.tx_id)
                {
                    if Some(*status) != txn.txn_type.get_preceding_txn_state() {
                        return Err(anyhow!(ProcessTransactionError::InvalidTransactionState(
                            txn.tx_id,
                            txn.txn_type,
                            *status,
                        )));
                    }
                    if *client_id != txn.client_id {
                        return Err(anyhow!(ProcessTransactionError::MissingTransaction(
                            txn.tx_id,
                            txn.client_id,
                            txn.txn_type,
                        )));
                    }

                    match txn.txn_type {
                        TransactionType::Dispute => {
                            if record.available_amounts >= *amount {
                                record.available_amounts -= *amount;
                                record.held_amounts += *amount;
                                *status = TransactionType::Dispute;
                            } else {
                                return Err(anyhow!(ProcessTransactionError::InsufficientFunds(
                                    txn.tx_id,
                                    txn.txn_type,
                                )));
                            }
                        }
                        TransactionType::Resolve => {
                            if record.held_amounts < *amount {
                                return Err(anyhow!("logic error: held funds should never be insufficient for a resolve"));
                            }

                            record.available_amounts += *amount;
                            record.held_amounts -= *amount;
                            *status = TransactionType::Resolve;
                        }
                        TransactionType::Chargeback => {
                            if record.held_amounts < *amount {
                                return Err(anyhow!("logic error: held funds should never be insufficient for a chargeback"));
                            }
                            record.held_amounts -= *amount;
                            *status = TransactionType::Chargeback;
                            record.is_locked = true
                        }
                        _ => unreachable!(),
                    };
                } else {
                    return Err(anyhow!(ProcessTransactionError::MissingTransaction(
                        txn.tx_id,
                        txn.client_id,
                        txn.txn_type,
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spectral::prelude::*;

    macro_rules! check_client {
        ($cr:ident, $id:literal, $aa:literal, $ha:literal) => {
            let client = $cr.view().get(&$id);
            assert_that!(client)
                .is_some()
                .map(|c| &c.available_amounts)
                .is_equal_to($aa);
            assert_that!(client)
                .is_some()
                .map(|c| &c.held_amounts)
                .is_equal_to($ha);
            assert_that!(client)
                .is_some()
                .matches(|c| c.available_amounts + c.held_amounts == c.total_amounts())
        };
    }

    #[test]
    fn it_should_process_a_single_deposit() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_process_a_deposit_followed_by_a_valid_withdrawal() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;

        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);
        let withdrawal_txn = Transaction::new_withdrawal_txn(client_id, 2, 5.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        assert_that(&client_records.process_transaction(&withdrawal_txn)).is_ok();

        check_client!(client_records, 1, 5.0, 0.0);
    }

    #[test]
    fn it_should_fail_to_process_a_withdrawal_if_there_are_insufficient_available_funds() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;

        let withdrawal_txn_1 = Transaction::new_withdrawal_txn(client_id, 1, 1.0);

        assert_that(&client_records.process_transaction(&withdrawal_txn_1))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::InsufficientFunds(
                    1,
                    TransactionType::Withdrawal,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 0.0, 0.0);

        let deposit_txn = Transaction::new_deposit_txn(client_id, 2, 10.0);
        let withdrawal_txn_2 = Transaction::new_withdrawal_txn(client_id, 3, 15.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        assert_that(&client_records.process_transaction(&withdrawal_txn_2))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::InsufficientFunds(
                    3,
                    TransactionType::Withdrawal,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_fail_process_the_same_deposit_twice_by_txn_id() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let deposit_txn_2 = Transaction::new_deposit_txn(client_id, 1, 123.0);

        assert_that(&client_records.process_transaction(&deposit_txn_2))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::DuplicateTransaction(1))
                    == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_fail_process_the_same_withdrawal_twice_by_txn_id() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let withdrawal_txn_1 = Transaction::new_withdrawal_txn(client_id, 2, 1.0);

        assert_that(&client_records.process_transaction(&withdrawal_txn_1)).is_ok();
        check_client!(client_records, 1, 9.0, 0.0);

        let withdrawal_txn_2 = Transaction::new_withdrawal_txn(client_id, 2, 5.0);

        assert_that(&client_records.process_transaction(&withdrawal_txn_2))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::DuplicateTransaction(2))
                    == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 9.0, 0.0);
    }

    #[test]
    fn it_should_be_able_to_dispute_a_deposit_txn() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 10.0);
    }

    #[test]
    fn it_should_fail_to_dispute_the_same_txn_twice() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 10.0);

        assert_that(&client_records.process_transaction(&dispute_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::InvalidTransactionState(
                    1,
                    TransactionType::Dispute,
                    TransactionType::Dispute,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 0.0, 10.0);
    }

    #[test]
    fn it_should_fail_to_dispute_a_txn_that_doesnt_exist() {
        let mut client_records = ClientRecords::new();

        let dispute_txn = Transaction::new_dispute_txn(1, 1);
        assert_that(&client_records.process_transaction(&dispute_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::MissingTransaction(
                    1,
                    1,
                    TransactionType::Dispute,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
    }

    #[test]
    fn it_should_fail_to_dispute_a_txn_that_doesnt_exist_for_the_client() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let dispute_txn = Transaction::new_dispute_txn(2, 1);
        assert_that(&client_records.process_transaction(&dispute_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::MissingTransaction(
                    1,
                    2,
                    TransactionType::Dispute,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_fail_to_dispute_a_txn_where_funds_are_insufficient() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let withdraw_txn = Transaction::new_withdrawal_txn(client_id, 2, 5.0);
        assert_that(&client_records.process_transaction(&withdraw_txn)).is_ok();
        check_client!(client_records, 1, 5.0, 0.0);

        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::InsufficientFunds(
                    1,
                    TransactionType::Dispute,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 5.0, 0.0);
    }

    #[test]
    fn it_should_be_able_to_resolve_a_disputed_txn() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);
        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 10.0);

        let resolve_txn = Transaction::new_resolve_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&resolve_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_fail_to_resolve_a_txn_that_doesnt_exist() {
        let mut client_records = ClientRecords::new();

        let resolve_txn = Transaction::new_resolve_txn(1, 1);
        assert_that(&client_records.process_transaction(&resolve_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::MissingTransaction(
                    1,
                    1,
                    TransactionType::Resolve,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
    }

    #[test]
    fn it_should_be_able_to_chargeback_a_disputed_transaction() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);
        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 10.0);

        let chargeback_txn = Transaction::new_chargeback_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&chargeback_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 0.0);
        assert_that!(client_records.view().get(&1))
            .is_some()
            .map(|c| &c.is_locked)
            .is_equal_to(true);
    }

    #[test]
    fn it_should_fail_to_chargeback_a_txn_that_doesnt_exist() {
        let mut client_records = ClientRecords::new();

        let chargeback_txn = Transaction::new_chargeback_txn(1, 1);
        assert_that(&client_records.process_transaction(&chargeback_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::MissingTransaction(
                    1,
                    1,
                    TransactionType::Chargeback,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
    }

    #[test]
    fn it_should_fail_to_chargeback_a_txn_where_it_has_already_been_resolved() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);

        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        check_client!(client_records, 1, 0.0, 10.0);

        let resolve_txn = Transaction::new_resolve_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&resolve_txn)).is_ok();
        check_client!(client_records, 1, 10.0, 0.0);

        let chargeback_txn = Transaction::new_chargeback_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&chargeback_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::InvalidTransactionState(
                    1,
                    TransactionType::Chargeback,
                    TransactionType::Resolve,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
        check_client!(client_records, 1, 10.0, 0.0);
    }

    #[test]
    fn it_should_fail_to_withdraw_if_client_account_is_locked() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);
        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();
        let dispute_txn = Transaction::new_dispute_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&dispute_txn)).is_ok();
        let chargeback_txn = Transaction::new_chargeback_txn(client_id, 1);
        assert_that(&client_records.process_transaction(&chargeback_txn)).is_ok();

        check_client!(client_records, 1, 0.0, 0.0);

        let deposit_txn_2 = Transaction::new_deposit_txn(client_id, 2, 5.0);
        assert_that(&client_records.process_transaction(&deposit_txn_2)).is_ok();

        let withdrawal_txn = Transaction::new_withdrawal_txn(1, 3, 2.0);
        assert_that(&client_records.process_transaction(&withdrawal_txn))
            .is_err()
            .matches(|e| {
                Some(&ProcessTransactionError::ClientAccountFrozen(
                    3,
                    TransactionType::Withdrawal,
                    1,
                )) == e.downcast_ref::<ProcessTransactionError>()
            });
    }

    #[test]
    fn it_should_ignore_failed_withdrawals_from_duplicate_tx_id_checks() {
        let mut client_records = ClientRecords::new();
        let client_id = 1;
        let deposit_txn = Transaction::new_deposit_txn(client_id, 1, 10.0);
        assert_that(&client_records.process_transaction(&deposit_txn)).is_ok();

        let withdrawal_txn = Transaction::new_withdrawal_txn(2, 2, 2.0);
        assert_that(&client_records.process_transaction(&withdrawal_txn)).is_err();

        let deposit_txn_2 = Transaction::new_deposit_txn(client_id, 2, 10.0);
        assert_that(&client_records.process_transaction(&deposit_txn_2)).is_ok();
    }
}
