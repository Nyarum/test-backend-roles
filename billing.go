package inferno

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var ErrInsufficientCredits = errors.New("insufficient credits")

func (d *Dispatcher) Charge(ctx context.Context, accountID, cost int64) error {
	tx, err := d.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var credits int64
	err = tx.QueryRow(ctx, `SELECT credits FROM accounts WHERE id = $1`, accountID).Scan(&credits)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("account %d not found", accountID)
	}
	if err != nil {
		return err
	}

	if credits < cost {
		return ErrInsufficientCredits
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET credits = $2 WHERE id = $1`, accountID, credits-cost)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
