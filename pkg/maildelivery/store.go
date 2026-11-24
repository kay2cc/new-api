package maildelivery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (d *Dispatcher) credentials(a model.MailAccount) (Credentials, error) {
	var c Credentials
	data, err := unseal(a.Secret, "account:"+a.Name)
	if err != nil {
		return c, err
	}
	err = common.Unmarshal(data, &c)
	return c, err
}

// refreshQuotas runs with the account row locked and no unresolved submissions.
// Provider balances never overwrite local reservations using an unlocked snapshot.
func (d *Dispatcher) refreshQuotas(ctx context.Context, tx *gorm.DB, a model.MailAccount, quotas []model.MailQuota) error {
	needsProvider := false
	for _, q := range quotas {
		if q.Mode == "provider_balance" || q.Mode == "provider_cycle" {
			needsProvider = true
		}
	}
	var observations []QuotaObservation
	if needsProvider {
		var count int64
		if err := tx.Model(&model.MailAttempt{}).Where("account_id = ? AND state IN ?", a.ID, []string{"sending", "unknown"}).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return errUnavailable
		}
		c, err := d.credentials(a)
		if err != nil {
			return errUnavailable
		}
		p := d.Providers[a.Provider]
		if p == nil {
			return errUnavailable
		}
		observations, err = p.Quotas(ctx, c)
		if err != nil {
			return errUnavailable
		}
	}
	now := d.Now()
	for i := range quotas {
		q := &quotas[i]
		switch q.Mode {
		case "calendar_day":
			loc, err := time.LoadLocation(q.Timezone)
			if err != nil {
				return err
			}
			t := now.In(loc)
			period := t.Format("2006-01-02")
			if q.Period != period {
				q.Period = period
				q.Used = 0
				q.Reserved = 0
				q.PeriodEnd = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc).Unix()
			}
		case "manual": // No fabricated reset for an unconfirmed provider calendar.
			if q.Period == "" || q.PeriodEnd > 0 && q.PeriodEnd <= now.Unix() {
				return errUnavailable
			}
		case "provider_balance", "provider_cycle":
			found := false
			for _, o := range observations {
				if o.Name != q.Name {
					continue
				}
				found = true
				if o.Remaining < 0 || o.Capacity <= 0 || o.Remaining > o.Capacity {
					return errUnavailable
				}
				if q.Capacity > 0 && q.Capacity != o.Capacity {
					return errUnavailable
				}
				if q.Mode == "provider_cycle" && (o.Period == "" || o.PeriodEnd <= now.Unix()) {
					return errUnavailable
				}
				// An increase replenishes the observed platform balance, not a guessed day.
				// Keep unreconciled local consumption when the provider's counter is stale.
				used := o.Capacity - o.Remaining
				if !q.Initialized || q.Mode == "provider_cycle" && q.Period != o.Period {
					q.Used = used
				} else if q.Mode == "provider_balance" && o.Remaining > q.Observed {
					// Retain locally accepted consumption that the platform has not yet
					// reflected. A stale higher balance must not erase that consumption.
					q.Used = max(used, q.Used-(o.Remaining-q.Observed))
				} else {
					q.Used = max(q.Used, used)
				}
				q.Initialized = true
				q.Capacity = o.Capacity
				q.Observed = o.Remaining
				if q.Mode == "provider_cycle" {
					q.Period = o.Period
					q.PeriodEnd = o.PeriodEnd
				} else if q.Period == "" {
					q.Period = "balance"
				}
			}
			if !found {
				return errUnavailable
			}
		default:
			return errUnavailable
		}
		if err := tx.Save(q).Error; err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) reserve(ctx context.Context, job model.MailJob, lease string, channel model.MailChannel, units int64) (*model.MailAttempt, model.MailAccount, error) {
	var a model.MailAccount
	attempt := &model.MailAttempt{ID: uuid.NewString(), JobID: job.ID, ChannelID: channel.ID, AccountID: channel.AccountID, State: "sending", FromAddress: channel.FromAddress, FromName: channel.FromName, Units: units, CreatedAt: d.Now().Unix(), UpdatedAt: d.Now().Unix()}
	err := d.DB.Transaction(func(tx *gorm.DB) error {
		// First statement is a write: serializes SQLite and obtains account locks on
		// MySQL/PostgreSQL without upgrading a deferred SQLite read transaction.
		if err := tx.Model(&model.MailAccount{}).Where("id = ?", channel.AccountID).UpdateColumn("cooldown_until", gorm.Expr("cooldown_until")).Error; err != nil {
			return err
		}
		if err := tx.First(&a, channel.AccountID).Error; err != nil {
			return err
		}
		if !a.Enabled || a.CooldownUntil > d.Now().Unix() {
			return errUnavailable
		}
		var current model.MailChannel
		if err := tx.First(&current, channel.ID).Error; err != nil {
			return err
		}
		attempt.FromAddress, attempt.FromName = current.FromAddress, current.FromName
		if !current.Enabled || current.AccountID != a.ID {
			return errUnavailable
		}
		var quotas []model.MailQuota
		if err := tx.Where("account_id = ?", a.ID).Order("id").Find(&quotas).Error; err != nil {
			return err
		}
		if len(quotas) == 0 {
			return errUnavailable
		} // Every account must have an explicit budget.
		if err := d.refreshQuotas(ctx, tx, a, quotas); err != nil {
			return err
		}
		for _, q := range quotas {
			if q.Limit <= 0 || q.Used+q.Reserved+units > q.Limit || q.Capacity > 0 && q.Used+q.Reserved+units > q.Capacity {
				return errUnavailable
			}
			if err := tx.Model(&model.MailQuota{}).Where("id = ?", q.ID).UpdateColumn("reserved", gorm.Expr("reserved + ?", units)).Error; err != nil {
				return err
			}
			r := model.MailReservation{AttemptID: attempt.ID, QuotaID: q.ID, Period: q.Period, Units: units}
			if err := tx.Create(&r).Error; err != nil {
				return err
			}
		}
		// Account lock may have waited for another worker. Revalidate lease/expiry.
		res := tx.Model(&model.MailJob{}).Where("id = ? AND lease = ? AND expires_at > ?", job.ID, lease, d.Now().Unix()).Updates(map[string]any{"state": "sending", "lease_until": d.Now().Add(2 * time.Minute).Unix(), "attempts": gorm.Expr("attempts + 1")})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return errUnavailable
		}
		return tx.Create(attempt).Error
	})
	return attempt, a, err
}

func (d *Dispatcher) settle(attempt model.MailAttempt, result Result) error {
	return d.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.MailAccount{}).Where("id = ?", attempt.AccountID).UpdateColumn("cooldown_until", gorm.Expr("cooldown_until")).Error; err != nil {
			return err
		}
		var stored model.MailAttempt
		if err := tx.First(&stored, "id = ?", attempt.ID).Error; err != nil {
			return err
		}
		if stored.State == "delivered" || stored.State == "rejected" || stored.State == "permanent" || stored.State == "failed" || stored.State == "cancelled" {
			return nil
		}
		var reservations []model.MailReservation
		if err := tx.Where("attempt_id = ?", attempt.ID).Find(&reservations).Error; err != nil {
			return err
		}
		// Accepted stays consumed even if a later bounce occurs: the provider may
		// charge it. Rejections only release reservations for unresolved submissions.
		if stored.State == "sending" || stored.State == "unknown" {
			for _, r := range reservations {
				if result.State == "unknown" || result.State == "pending" {
					continue
				}
				updates := map[string]any{"reserved": gorm.Expr("reserved - ?", r.Units)}
				if result.State == "accepted" || result.State == "delivered" || result.State == "failed" || result.State == "permanent" {
					updates["used"] = gorm.Expr("used + ?", r.Units)
				}
				if err := tx.Model(&model.MailQuota{}).Where("id = ? AND period = ? AND reserved >= ?", r.QuotaID, r.Period, r.Units).Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		state := result.State
		if state == "pending" {
			state = stored.State
		}
		changes := map[string]any{"state": state, "code": result.Code, "updated_at": d.Now().Unix()}
		if result.ID != "" {
			changes["provider_id"] = result.ID
		}
		if err := tx.Model(&stored).Updates(changes).Error; err != nil {
			return err
		}
		if result.State == "rejected" || result.State == "failed" {
			return tx.Model(&model.MailAccount{}).Where("id = ?", attempt.AccountID).Update("cooldown_until", d.Now().Add(time.Minute).Unix()).Error
		}
		return nil
	})
}

func (d *Dispatcher) enqueue(r common.EmailDeliveryRequest) error {
	m, err := parseMessage(r)
	if err != nil {
		return common.NewMessage("Invalid email address")
	}
	now := d.Now()
	if !r.ExpiresAt.After(now) || r.ExpiresAt.After(now.Add(24*time.Hour+time.Second)) {
		return common.NewMessage("Invalid parameters")
	}
	// One job per recipient makes partial acceptance and delivery tracking unambiguous.
	return d.DB.Transaction(func(tx *gorm.DB) error {
		for _, recipient := range m.To {
			single := m
			single.To = []string{recipient}
			raw, err := common.Marshal(single)
			if err != nil {
				return err
			}
			job := model.MailJob{ID: uuid.NewString(), State: "queued", NextAttempt: now.Unix(), ExpiresAt: r.ExpiresAt.Unix(), CreatedAt: now.Unix(), ValidityTag: r.ValidityTag}
			if r.ValidityTag != "" && !strings.HasPrefix(r.ValidityTag, "binding:") {
				job.ProcessID = d.ProcessID
			}
			ciphertext, err := seal(raw, "job:"+job.ID)
			job.Payload = model.MailPayload(ciphertext)
			if err != nil {
				return common.NewMessage("Verification email could not be sent. Start email verification again.")
			}
			if err = tx.Create(&job).Error; err != nil {
				return common.NewMessage("Verification email could not be sent. Start email verification again.")
			}
		}
		return nil
	})
}

func (d *Dispatcher) finish(job model.MailJob, lease, state, code string, next int64) error {
	changes := map[string]any{"state": state, "last_code": code, "next_attempt": next, "lease": "", "lease_until": 0}
	if state == "delivered" || state == "expired" || state == "failed" || state == "submitted" {
		changes["payload"] = ""
	}
	return d.DB.Model(&model.MailJob{}).Where("id = ? AND lease = ?", job.ID, lease).Updates(changes).Error
}

func quotaError(name string) error { return fmt.Errorf("mail quota %s could not be refreshed", name) }

// resetPeriod is only for a provider-confirmed manual period. It refuses to
// erase outstanding reservations or turn a calendar/balance rule into manual.
func (d *Dispatcher) resetPeriod(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("reset-period requires QUOTA_ID PROVIDER_PERIOD END_UNIX")
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}
	end, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || end <= d.Now().Unix() || args[1] == "" || len(args[1]) > 100 {
		return fmt.Errorf("invalid provider period")
	}
	var q model.MailQuota
	if err := d.DB.First(&q, id).Error; err != nil {
		return err
	}
	return d.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.MailAccount{}).Where("id = ?", q.AccountID).UpdateColumn("cooldown_until", gorm.Expr("cooldown_until")).Error; err != nil {
			return err
		}
		if err := tx.First(&q, id).Error; err != nil {
			return err
		}
		if q.Mode != "manual" || q.Reserved != 0 || q.Period == args[1] {
			return fmt.Errorf("quota cannot be reset")
		}
		return tx.Model(&q).Updates(map[string]any{"period": args[1], "period_end": end, "used": 0}).Error
	})
}
