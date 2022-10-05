use std::fmt;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[readonly::make]
pub struct Transaction {
    #[serde(rename = "type")]
    pub txn_type: TransactionType,

    #[serde(rename = "client")]
    pub client_id: u16,

    #[serde(rename = "tx")]
    pub tx_id: u32,

    pub amount: Option<f32>,
}

impl Transaction {
    pub fn new_deposit_txn(client_id: u16, tx_id: u32, amount: f32) -> Transaction {
        Transaction {
            txn_type: TransactionType::Deposit,
            client_id,
            tx_id,
            amount: Some(amount),
        }
    }

    pub fn new_withdrawal_txn(client_id: u16, tx_id: u32, amount: f32) -> Transaction {
        Transaction {
            txn_type: TransactionType::Withdrawal,
            client_id,
            tx_id,
            amount: Some(amount),
        }
    }

    pub fn new_dispute_txn(client_id: u16, tx_id: u32) -> Transaction {
        Transaction {
            txn_type: TransactionType::Dispute,
            client_id,
            tx_id,
            amount: None,
        }
    }

    pub fn new_resolve_txn(client_id: u16, tx_id: u32) -> Transaction {
        Transaction {
            txn_type: TransactionType::Resolve,
            client_id,
            tx_id,
            amount: None,
        }
    }

    pub fn new_chargeback_txn(client_id: u16, tx_id: u32) -> Transaction {
        Transaction {
            txn_type: TransactionType::Chargeback,
            client_id,
            tx_id,
            amount: None,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
pub enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Deposit => write!(f, "deposit"),
            TransactionType::Withdrawal => write!(f, "withdrawal"),
            TransactionType::Dispute => write!(f, "dispute"),
            TransactionType::Resolve => write!(f, "resolve"),
            TransactionType::Chargeback => write!(f, "chargeback"),
        }
    }
}

impl TransactionType {
    pub(super) fn get_preceding_txn_state(&self) -> Option<TransactionType> {
        match self {
            TransactionType::Deposit => None,
            TransactionType::Withdrawal => None,
            TransactionType::Dispute => Some(TransactionType::Deposit),
            TransactionType::Resolve | TransactionType::Chargeback => {
                Some(TransactionType::Dispute)
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Deposit {
    pub(super) client_id: u16,
    pub(super) amount: f32,
    pub(super) status: TransactionType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use spectral::prelude::*;

    #[test]
    fn test_preceding_txn_state() {
        assert_that!(TransactionType::Deposit.get_preceding_txn_state()).is_equal_to(None);
        assert_that!(TransactionType::Withdrawal.get_preceding_txn_state()).is_equal_to(None);
        assert_that!(TransactionType::Dispute.get_preceding_txn_state())
            .is_equal_to(Some(TransactionType::Deposit));
        assert_that!(TransactionType::Resolve.get_preceding_txn_state())
            .is_equal_to(Some(TransactionType::Dispute));
        assert_that!(TransactionType::Chargeback.get_preceding_txn_state())
            .is_equal_to(Some(TransactionType::Dispute));
    }
}
