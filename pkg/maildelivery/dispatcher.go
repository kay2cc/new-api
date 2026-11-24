package maildelivery

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Install is opt-in. An existing installation continues using its selected
// legacy transport until explicitly switched to database mail routing.
func Install(ctx context.Context, db *gorm.DB) (func(), error) {
	if os.Getenv("MAIL_DELIVERY_ENABLED") != "true" {
		return func() {}, nil
	}
	if _, err := encryptionKey(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	d := New(db.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
	wake := make(chan struct{}, 1)
	common.EmailDeliveryHandler = func(r common.EmailDeliveryRequest) error {
		if err := d.enqueue(r); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
		return nil
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if err := d.RunOnce(ctx); err != nil {
				common.SysError(common.LogText("Mail queue processing failed"))
			}
			select {
			case <-ctx.Done():
				return
			case <-wake:
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}

// RunOnce claims jobs with compare-and-swap leases; it is safe across processes.
func (d *Dispatcher) RunOnce(ctx context.Context) error {
	now := d.Now().Unix()
	// Expired payloads are purged even when their owning OTP process has restarted.
	if err := d.DB.Model(&model.MailJob{}).Where("expires_at <= ? AND payload <> ? AND lease_until <= ?", now, "", now).
		Updates(map[string]any{"payload": "", "state": "expired"}).Error; err != nil {
		return err
	}
	unresolved := d.DB.Model(&model.MailAttempt{}).Select("job_id").Where("state IN ?", []string{"sending", "unknown", "accepted"})
	var jobs []model.MailJob
	if err := d.DB.Where("next_attempt <= ? AND lease_until <= ?", now, now).
		Where("(state IN ? AND expires_at > ? AND (process_id = ? OR process_id = ?)) OR (state = ? AND id IN (?))",
			[]string{"queued", "accepted", "unknown", "sending"}, now, "", d.ProcessID, "expired", unresolved).
		Order("next_attempt, created_at, id").Limit(20).Find(&jobs).Error; err != nil {
		return err
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := d.Process(ctx, job.ID); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) Process(ctx context.Context, id string) error {
	now := d.Now()
	lease := uuid.NewString()
	claim := d.DB.Model(&model.MailJob{}).Where("id = ? AND lease_until <= ? AND state IN ?", id, now.Unix(), []string{"queued", "accepted", "unknown", "sending", "expired"}).
		Updates(map[string]any{"lease": lease, "lease_until": now.Add(2 * time.Minute).Unix()})
	if claim.Error != nil {
		return claim.Error
	}
	if claim.RowsAffected != 1 {
		return nil
	}
	var job model.MailJob
	if err := d.DB.First(&job, "id = ?", id).Error; err != nil {
		return err
	}
	canSend, err := d.deliveryCurrent(job)
	if err != nil {
		return err
	}
	var message Message
	if canSend {
		plain, err := unseal(string(job.Payload), "job:"+job.ID)
		if err != nil {
			return d.finish(job, lease, "queued", "payload_unavailable", d.Now().Add(time.Minute).Unix())
		}
		if common.Unmarshal(plain, &message) != nil {
			return d.finish(job, lease, "failed", "payload_invalid", 0)
		}
	}
	var attempts []model.MailAttempt
	if err := d.DB.Where("job_id = ?", id).Order("created_at, id").Find(&attempts).Error; err != nil {
		return err
	}
	for _, attempt := range attempts {
		// An operator may already have reconciled an ambiguous attempt.
		if attempt.State == "delivered" {
			return d.finish(job, lease, "delivered", "", 0)
		}
		if attempt.State == "permanent" {
			return d.finish(job, lease, "failed", attempt.Code, 0)
		}
		if attempt.State != "sending" && attempt.State != "unknown" && attempt.State != "accepted" {
			continue
		}
		var account model.MailAccount
		if err := d.DB.First(&account, attempt.AccountID).Error; err != nil {
			return err
		}
		credentials, err := d.credentials(account)
		provider := d.Providers[account.Provider]
		if err != nil || provider == nil {
			return d.waitForOutcome(job, lease, canSend, attempt.State, "provider_configuration")
		}
		lookupCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		result := provider.Lookup(lookupCtx, credentials, attempt)
		cancel()
		// Reuse the exact sender, content and attempt key within Resend's documented
		// idempotency window. Expired or invalid challenges are never replayed.
		if canSend {
			canSend, err = d.deliveryCurrent(job)
			if err != nil {
				return err
			}
		}
		if canSend && attempt.ProviderID == "" && account.Provider == "resend" && now.Unix()-attempt.CreatedAt < 23*3600 && result.State == "pending" {
			ch := model.MailChannel{FromAddress: attempt.FromAddress, FromName: attempt.FromName}
			sendCtx, stop := context.WithTimeout(ctx, 25*time.Second)
			result = provider.Send(sendCtx, credentials, ch, message, attempt.ID)
			stop()
		}
		result, err = d.applyResult(attempt, result)
		if err != nil {
			return err
		}
		switch result.State {
		case "delivered":
			return d.finish(job, lease, "delivered", "", 0)
		case "permanent":
			return d.finish(job, lease, "failed", result.Code, 0)
		case "failed", "rejected": // Definite failure: immediately try another account.
		default:
			state := attempt.State
			if result.State == "accepted" {
				state = "accepted"
			}
			return d.waitForOutcome(job, lease, canSend, state, result.Code)
		}
	}
	if !canSend {
		return d.finish(job, lease, "expired", "expired", 0)
	}
	var channels []model.MailChannel
	if err := d.DB.Where("enabled = ?", true).Order("priority, id").Find(&channels).Error; err != nil {
		return err
	}
	tried := map[int64]bool{}
	for _, ch := range channels {
		if tried[ch.AccountID] {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		valid, err := d.deliveryCurrent(job)
		if err != nil {
			return err
		}
		if !valid {
			return d.finish(job, lease, "expired", "expired", 0)
		}
		sendCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		attempt, account, err := d.reserve(sendCtx, job, lease, ch, 1)
		if err != nil {
			cancel()
			if !errors.Is(err, errUnavailable) {
				return err
			}
			continue
		}
		valid, err = d.deliveryCurrent(job)
		if err != nil || !valid {
			cancel()
			if settleErr := d.settle(*attempt, Result{State: "cancelled", Code: "challenge_invalid"}); settleErr != nil {
				return settleErr
			}
			if err != nil {
				return err
			}
			return d.finish(job, lease, "expired", "expired", 0)
		}
		tried[ch.AccountID] = true
		provider := d.Providers[account.Provider]
		credentials, err := d.credentials(account)
		result := Result{State: "rejected", Code: "provider_configuration"}
		if err == nil && provider != nil {
			snapshot := ch
			snapshot.FromAddress, snapshot.FromName = attempt.FromAddress, attempt.FromName
			result = provider.Send(sendCtx, credentials, snapshot, message, attempt.ID)
		}
		cancel()
		result, err = d.applyResult(*attempt, result)
		if err != nil {
			return err
		}
		switch result.State {
		case "accepted":
			if account.Provider == "smtp" {
				return d.finish(job, lease, "submitted", "smtp_acknowledged", 0)
			}
			return d.finish(job, lease, "accepted", "", d.Now().Add(20*time.Second).Unix())
		case "delivered":
			return d.finish(job, lease, "delivered", "", 0)
		case "permanent":
			return d.finish(job, lease, "failed", result.Code, 0)
		case "unknown", "pending", "sending":
			return d.waitForOutcome(job, lease, true, "unknown", result.Code)
		}
	}
	return d.finish(job, lease, "queued", "no_available_channel", d.Now().Add(time.Minute).Unix())
}

func (d *Dispatcher) waitForOutcome(job model.MailJob, lease string, canSend bool, state, code string) error {
	if !canSend {
		return d.finish(job, lease, "expired", code, d.Now().Add(time.Minute).Unix())
	}
	if state != "accepted" {
		state = "unknown"
	}
	return d.finish(job, lease, state, code, d.Now().Add(20*time.Second).Unix())
}

// deliveryCurrent is rechecked after quota locks and provider lookups, which can
// wait long enough for a verification challenge to be replaced or expire.
func (d *Dispatcher) deliveryCurrent(job model.MailJob) (bool, error) {
	if job.ExpiresAt <= d.Now().Unix() || job.ProcessID != "" && job.ProcessID != d.ProcessID {
		return false, nil
	}
	if job.ValidityTag == "" {
		return true, nil
	}
	return d.Valid(job.ValidityTag)
}

// applyResult reads the committed outcome so a stale lookup cannot override an
// already resolved attempt and accidentally trigger a second provider send.
func (d *Dispatcher) applyResult(attempt model.MailAttempt, result Result) (Result, error) {
	if err := d.settle(attempt, result); err != nil {
		return Result{}, err
	}
	var stored model.MailAttempt
	if err := d.DB.First(&stored, "id = ?", attempt.ID).Error; err != nil {
		return Result{}, err
	}
	return Result{State: stored.State, ID: stored.ProviderID, Code: stored.Code}, nil
}
