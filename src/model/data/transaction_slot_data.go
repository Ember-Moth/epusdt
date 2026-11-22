package data

import (
	"strings"
	"time"

	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type slotAddress struct {
	input string // returned to the caller unchanged
	key   string // the address stored in and compared against transaction_lock
}

type slotAmount struct {
	value  float64
	scaled int64
	text   string
	key    string // precision-independent decimal value
}

func slotKey(address, amount string) string {
	return address + "\x00" + amount
}

// ReserveTransactionSlot reserves the first free address and amount for one
// order in a single runtime transaction. Candidates are tried in the same
// order as repeated LockTransaction calls: every address at amounts[0], then
// every address at amounts[1], and so on. It returns an empty address when
// every candidate is taken.
//
// Active locks for the network and token are read once and matched in memory
// by address and decimal amount, so a lock stored at another precision still
// blocks its value. The insert relies on
// transaction_lock_network_address_token_amount_uindex; a conflict marks that
// candidate taken and the search continues.
func ReserveTransactionSlot(network, token, tradeID string, addresses []string, amounts []float64, expiration time.Duration) (string, float64, error) {
	network = normalizeLockNetwork(network)
	token = normalizeLockToken(token)
	precision := GetAmountPrecision()
	evm := isEVMNetwork(network)

	candidates := make([]slotAddress, 0, len(addresses))
	wanted := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		key := normalizeLockAddress(network, address)
		if wanted[key] {
			continue
		}
		wanted[key] = true
		candidates = append(candidates, slotAddress{input: address, key: key})
	}
	slots := make([]slotAmount, len(amounts))
	for i, amount := range amounts {
		scaled, text := normalizeAmount(amount, precision)
		value, err := decimal.NewFromString(text)
		if err != nil {
			return "", 0, err
		}
		slots[i] = slotAmount{value: amount, scaled: scaled, text: text, key: value.String()}
	}
	if len(candidates) == 0 || len(slots) == 0 {
		return "", 0, nil
	}

	var reservedAddress string
	var reservedAmount float64
	now := time.Now()
	err := dao.RuntimeDB.Transaction(func(tx *gorm.DB) error {
		// Expired rows are invisible to every reader, and a reused target needs
		// its expired row gone before the unique index accepts the new owner.
		if err := tx.Where("network = ? AND token = ? AND expires_at <= ?", network, token, now).
			Delete(&mdb.TransactionLock{}).Error; err != nil {
			return err
		}
		if err := tx.Where("trade_id = ?", tradeID).Delete(&mdb.TransactionLock{}).Error; err != nil {
			return err
		}
		var active []mdb.TransactionLock
		if err := tx.Model(&mdb.TransactionLock{}).
			Select("address", "amount_scaled", "amount_text", "amount_precision").
			Where("network = ? AND token = ? AND expires_at > ?", network, token, now).
			Find(&active).Error; err != nil {
			return err
		}
		taken := make(map[string]bool, len(active))
		for _, lock := range active {
			address := lock.Address
			if evm {
				address = strings.ToLower(address)
			}
			if wanted[address] {
				taken[slotKey(address, lockAmountDecimal(lock).String())] = true
			}
		}
		for _, slot := range slots {
			for _, candidate := range candidates {
				key := slotKey(candidate.key, slot.key)
				if taken[key] {
					continue
				}
				result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&mdb.TransactionLock{
					Network:         network,
					Address:         candidate.key,
					Token:           token,
					AmountScaled:    slot.scaled,
					AmountText:      slot.text,
					AmountPrecision: precision,
					TradeId:         tradeID,
					ExpiresAt:       now.Add(expiration),
				})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected > 0 {
					reservedAddress, reservedAmount = candidate.input, slot.value
					return nil
				}
				taken[key] = true
			}
		}
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	return reservedAddress, reservedAmount, nil
}
