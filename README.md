# MYSTERIOUS UNNAMED RUST PROJECT

- Author: Ryan Lee
- Date: 10/4/22

## Usage

`cargo run -- [[YOUR_CSV]].csv`

## Comments

In terms of __efficiency__, I'm using a buffered reader to read the file so it shouldn't be memory intensive since it just seeks through the file instead of loading it through memory. Additionally, `parse_csv` function is agnostic to whether its a file or a socket, etc... because it takes `impl Read` as an input parameter.

In terms of __correctness__, I have various assumptions about edge cases that were not documented in [[REDACTED]]. These are tested in the unit tests.

1. If the same transaction id appears as a deposit or withdrawal, I ignore it.
2. If the same transaction id appears but use a different client id than the previous same transaction, I ignore it.
3. If a withdrawal fails, then the transaction id associated with the withdrawal is not considered "used".
4. If a deposit is disputed but there is not enough funds avaliable because the client has already withdrawn those funds, the dispute is ignored.
5. Disputing the same transaction twice is ignored.
6. If the client's account is locked then the client is unable to withdraw, only deposit.
7. Chargebacking a transaction that also already been resolved is a considered a failure and is ignored.
8. If an invalid transaction specifies a client that does not exist yet, it is still included in the output with a zero balance for everything.

I also used the `test.csv` file to test certain edge cases like whitespace and sending invalid transactions. In this test, there seems to be a rounding error where I deposited: `1337.1234`, withdrew `0.1233`, and then deposited `1000`. The result will show as `2337.0000` instead of `2337.0001`. Unsure why.

In terms of __safety__, I have created a custom error type to handle any common errors/edgecases listed above. In the main function I wrote a comment on how those errors can be handled. Anything outside of this, such as a logic error will just crash the program, which I think is appropriate because it means something is actually broken.
