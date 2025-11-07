-- The trigger runs AFTER INSERT so the transaction row has an id we can use when inserting ledger entries.
-- It performs:
--  - validation (existence, sufficient funds for withdrawal/transfer),
--  - updates account balances (atomic within the same DB transaction),
--  - updates transactions.balance_after,
--  - inserts two ledger_entries per transaction (double-entry).

CREATE OR REPLACE FUNCTION update_account_balance()
RETURNS TRIGGER AS $$
DECLARE
    src_balance NUMERIC(18,2);
BEGIN
    -- Validate account exists
    IF NOT EXISTS (SELECT 1 FROM accounts WHERE id = NEW.account_id) THEN
        RAISE EXCEPTION 'Account % does not exist', NEW.account_id;
    END IF;

    -- DEPOSIT: credit the account (increase balance)
    IF NEW.txn_type = 'DEPOSIT' THEN
        -- update balance
        UPDATE accounts
        SET balance = balance + NEW.amount
        WHERE id = NEW.account_id;

        -- snapshot new balance
        SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id;

        -- ledger: account receives DEBIT (asset increase), system/EXTERNAL gets CREDIT (account_id NULL)
        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NEW.account_id, 'DEBIT', NEW.amount);

        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NULL, 'CREDIT', NEW.amount);

    ELSIF NEW.txn_type = 'WITHDRAWAL' THEN
        -- Lock the row to avoid races
        SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id FOR UPDATE;
        IF src_balance IS NULL THEN
            RAISE EXCEPTION 'Account % does not exist', NEW.account_id;
        END IF;
        IF src_balance < NEW.amount THEN
            RAISE EXCEPTION 'Insufficient funds in account % (have: %, need: %)', NEW.account_id, src_balance, NEW.amount;
        END IF;

        UPDATE accounts
        SET balance = balance - NEW.amount
        WHERE id = NEW.account_id;

        SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id;

        -- ledger: account gets CREDIT (asset decrease), system gets DEBIT
        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NEW.account_id, 'CREDIT', NEW.amount);

        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NULL, 'DEBIT', NEW.amount);

    ELSIF NEW.txn_type = 'TRANSFER' THEN
        -- Validate destination exists
        IF NEW.related_account_id IS NULL THEN
            RAISE EXCEPTION 'related_account_id is required for TRANSFER';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM accounts WHERE id = NEW.related_account_id) THEN
            RAISE EXCEPTION 'Destination account % does not exist', NEW.related_account_id;
        END IF;

        -- Lock both rows (in a consistent order to avoid deadlocks - lock smaller id first)
        IF NEW.account_id < NEW.related_account_id THEN
            SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id FOR UPDATE;
            -- also lock dest
            PERFORM 1 FROM accounts WHERE id = NEW.related_account_id FOR UPDATE;
        ELSE
            PERFORM 1 FROM accounts WHERE id = NEW.related_account_id FOR UPDATE;
            SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id FOR UPDATE;
        END IF;

        IF src_balance IS NULL THEN
            RAISE EXCEPTION 'Source account % does not exist', NEW.account_id;
        END IF;
        IF src_balance < NEW.amount THEN
            RAISE EXCEPTION 'Insufficient funds in source account % (have: %, need: %)', NEW.account_id, src_balance, NEW.amount;
        END IF;

        -- debit source
        UPDATE accounts
        SET balance = balance - NEW.amount
        WHERE id = NEW.account_id;

        -- credit destination
        UPDATE accounts
        SET balance = balance + NEW.amount
        WHERE id = NEW.related_account_id;

        -- snapshot new balance of source (post-debit)
        SELECT balance INTO src_balance FROM accounts WHERE id = NEW.account_id;

        -- ledger: credit source, debit destination (double-entry)
        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NEW.account_id, 'CREDIT', NEW.amount);

        INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount)
        VALUES (NEW.id, NEW.related_account_id, 'DEBIT', NEW.amount);

    ELSE
        RAISE EXCEPTION 'Unsupported txn_type: %', NEW.txn_type;
    END IF;

    -- persist balance_after
    UPDATE transactions
    SET balance_after = src_balance
    WHERE id = NEW.id;

    RETURN NULL; -- AFTER trigger returns NULL
END;
$$ LANGUAGE plpgsql;

-- ---------- Trigger binding (AFTER INSERT) ----------

CREATE TRIGGER trg_update_balance
AFTER INSERT ON transactions
FOR EACH ROW
EXECUTE FUNCTION update_account_balance();
