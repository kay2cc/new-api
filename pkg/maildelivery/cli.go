package maildelivery

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/joho/godotenv"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Only fixed validation messages may be printed by the CLI; database and JSON
// errors can contain submitted data and are deliberately redacted.
type configurationError string

func (e configurationError) Error() string { return string(e) }

type AccountInput struct {
	Name        string         `json:"name"`
	Provider    string         `json:"provider"`
	Enabled     bool           `json:"enabled"`
	Credentials Credentials    `json:"credentials"`
	Quotas      []QuotaInput   `json:"quotas"`
	Channels    []ChannelInput `json:"channels"`
}
type QuotaInput struct {
	InitialUsed int64  `json:"initial_used"`
	Name        string `json:"name"`
	Mode        string `json:"mode"`
	Limit       int64  `json:"limit"`
	Capacity    int64  `json:"capacity"`
	Timezone    string `json:"timezone"`
	Period      string `json:"period"`
	PeriodEnd   int64  `json:"period_end"`
}
type ChannelInput struct {
	Name        string `json:"name"`
	FromAddress string `json:"from_address"`
	FromName    string `json:"from_name"`
	Priority    int    `json:"priority"`
	Enabled     bool   `json:"enabled"`
}

// Import uses stdin so credentials do not appear in command arguments or output.
// Existing usage is retained; a configuration edit is never a quota reset.
func (d *Dispatcher) Import(accounts []AccountInput) error {
	if len(accounts) == 0 || len(accounts) > 100 {
		return configurationError("provide between 1 and 100 mail accounts")
	}
	for _, a := range accounts {
		if strings.TrimSpace(a.Name) == "" || len(a.Name) > 100 || d.Providers[a.Provider] == nil || len(a.Quotas) == 0 || len(a.Channels) == 0 {
			return configurationError("invalid mail account configuration")
		}
		if len(a.Credentials.APIKey) > 8192 || len(a.Credentials.Host) > 253 || len(a.Credentials.Username) > 1024 || len(a.Credentials.Password) > 8192 {
			return configurationError("mail credentials exceed size limit")
		}
		if a.Provider != "smtp" && (strings.TrimSpace(a.Credentials.APIKey) == "" || strings.ContainsAny(a.Credentials.APIKey, "\r\n\x00")) {
			return configurationError("API key required")
		}
		if a.Provider == "smtp" && (a.Credentials.Host == "" || strings.ContainsAny(a.Credentials.Host, "\r\n\x00") || a.Credentials.Port < 1 || a.Credentials.Port > 65535 || a.Credentials.TLS != "starttls" && a.Credentials.TLS != "implicit") {
			return configurationError("SMTP requires host, port and starttls or implicit TLS")
		}
		names := map[string]bool{}
		for _, q := range a.Quotas {
			if q.InitialUsed < 0 || q.InitialUsed > 1_000_000_000 || q.Capacity > 0 && q.InitialUsed > q.Capacity || q.Name == "" || len(q.Name) > 40 || names[q.Name] || q.Limit <= 0 || q.Limit > 1_000_000_000 || q.Capacity < 0 || q.Capacity > 1_000_000_000 || q.Capacity > 0 && q.Limit > q.Capacity {
				return configurationError("invalid mail quota")
			}
			names[q.Name] = true
			switch q.Mode {
			case "calendar_day":
				// Only the Resend UTC boundary has been confirmed. Future adapters may
				// add documented calendar rules rather than defaulting to local midnight.
				if a.Provider != "resend" || q.Timezone != "UTC" || q.Name != "daily" {
					return configurationError("calendar rule is not verified for this provider")
				}
			case "provider_balance":
				if a.Provider != "brevo" || q.Name != "daily" || q.Capacity != 300 {
					return configurationError("unsupported provider balance rule")
				}
			case "provider_cycle":
				if a.Provider != "smtp2go" || q.Name != "monthly" {
					return configurationError("unsupported provider cycle rule")
				}
			case "manual":
				if q.Period == "" || len(q.Period) > 100 {
					return configurationError("manual quota requires a provider-confirmed period identifier")
				}
			default:
				return configurationError("unknown mail quota mode")
			}
		}
		for _, ch := range a.Channels {
			addr, err := mail.ParseAddress(ch.FromAddress)
			if err != nil || addr.Address != ch.FromAddress || ch.Name == "" || len(ch.Name) > 100 || len(ch.FromAddress) > 254 || len(ch.FromName) > 100 || strings.ContainsAny(ch.FromName, "\r\n\x00") {
				return configurationError("invalid mail channel")
			}
		}
	}
	return d.DB.Transaction(func(tx *gorm.DB) error {
		for _, input := range accounts {
			var a model.MailAccount
			if err := tx.Model(&model.MailAccount{}).Where("name = ?", input.Name).UpdateColumn("cooldown_until", gorm.Expr("cooldown_until")).Error; err != nil {
				return err
			}
			err := tx.Where("name = ?", input.Name).First(&a).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if a.ID != 0 {
				if err = tx.Model(&a).UpdateColumn("cooldown_until", gorm.Expr("cooldown_until")).Error; err != nil {
					return err
				}
				var active int64
				if err = tx.Model(&model.MailAttempt{}).Where("account_id = ? AND state IN ?", a.ID, []string{"sending", "unknown"}).Count(&active).Error; err != nil {
					return err
				}
				if active > 0 {
					return configurationError("account has unresolved deliveries; reconcile before changing credentials or configuration")
				}
				if a.Provider != input.Provider {
					return configurationError("an existing account cannot change provider")
				}
			}
			raw, err := common.Marshal(input.Credentials)
			if err != nil {
				return err
			}
			ciphertext, err := seal(raw, "account:"+input.Name)
			if err != nil {
				return err
			}
			a.Name = input.Name
			a.Provider = input.Provider
			a.Secret = ciphertext
			a.Enabled = input.Enabled
			if err = tx.Save(&a).Error; err != nil {
				return err
			}
			for _, q := range input.Quotas {
				var stored model.MailQuota
				err = tx.Where("account_id = ? AND name = ?", a.ID, q.Name).First(&stored).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if stored.ID != 0 && (stored.Mode != q.Mode || stored.Timezone != q.Timezone || q.Mode == "manual" && stored.Period != q.Period) {
					return configurationError("changing a quota period requires explicit reset-period command")
				}
				if stored.ID == 0 {
					stored.Used = q.InitialUsed
					if q.Mode == "calendar_day" {
						now := d.Now().UTC()
						stored.Period = now.Format("2006-01-02")
						stored.PeriodEnd = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC).Unix()
					}
				}
				stored.AccountID = a.ID
				stored.Name = q.Name
				stored.Mode = q.Mode
				stored.Limit = q.Limit
				stored.Capacity = q.Capacity
				stored.Timezone = q.Timezone
				if stored.Period == "" {
					stored.Period = q.Period
					stored.PeriodEnd = q.PeriodEnd
				}
				if err = tx.Save(&stored).Error; err != nil {
					return err
				}
			}
			for _, ch := range input.Channels {
				var stored model.MailChannel
				err = tx.Where("name = ?", ch.Name).First(&stored).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if stored.ID != 0 && stored.AccountID != a.ID {
					return configurationError("channel belongs to a different account")
				}
				stored.AccountID = a.ID
				stored.Name = ch.Name
				stored.FromAddress = ch.FromAddress
				stored.FromName = ch.FromName
				stored.Priority = ch.Priority
				stored.Enabled = ch.Enabled
				if err = tx.Save(&stored).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func RunCLI(args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(out, "new-api mail keygen | import | list | jobs | enqueue | enable ACCOUNT_ID true|false | attempts JOB_ID | run | resolve ATTEMPT_ID delivered|rejected | reset-period QUOTA_ID PROVIDER_PERIOD END_UNIX\nimport reads a JSON array of accounts from stdin. Stored credentials and message bodies are never printed. MAIL_ENCRYPTION_KEY and database settings are read from the environment/.env.")
		return 0
	}
	if args[0] == "keygen" {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return 1
		}
		fmt.Fprintln(out, base64.StdEncoding.EncodeToString(key))
		return 0
	}
	_ = godotenv.Load(".env")
	common.InitEnv()
	if err := model.InitDB(); err != nil {
		fmt.Fprintln(errOut, "mail database initialization failed")
		return 1
	}
	sqlDB, err := model.DB.DB()
	if err != nil {
		fmt.Fprintln(errOut, "mail database initialization failed")
		return 1
	}
	defer sqlDB.Close()
	// GORM diagnostics must not print serialized mail content or credentials.
	d := New(model.DB.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}))
	var result any
	err = nil
	switch args[0] {
	case "import":
		var accounts []AccountInput
		err = common.DecodeJson(io.LimitReader(in, 2*1024*1024), &accounts)
		if err == nil {
			err = d.Import(accounts)
		}
		result = "mail accounts imported"
	case "list":
		var accounts []model.MailAccount
		var channels []model.MailChannel
		var quotas []model.MailQuota
		err = d.DB.Find(&accounts).Error
		if err == nil {
			err = d.DB.Find(&channels).Error
		}
		if err == nil {
			err = d.DB.Find(&quotas).Error
		}
		result = map[string]any{"accounts": accounts, "channels": channels, "quotas": quotas}
	case "enable":
		if len(args) != 3 {
			fmt.Fprintln(errOut, "enable requires ACCOUNT_ID true|false")
			return 1
		}
		var id int64
		var enabled bool
		id, err = strconv.ParseInt(args[1], 10, 64)
		if err == nil {
			enabled, err = strconv.ParseBool(args[2])
		}
		if err == nil {
			res := d.DB.Model(&model.MailAccount{}).Where("id = ?", id).Update("enabled", enabled)
			err = res.Error
			if err == nil && res.RowsAffected != 1 {
				err = errors.New("account not found")
			}
		}
		result = "account availability updated"
	case "enqueue":
		var input struct {
			Subject  string `json:"subject"`
			Receiver string `json:"receiver"`
			Content  string `json:"content"`
			TTL      int64  `json:"ttl_seconds"`
		}
		err = common.DecodeJson(io.LimitReader(in, 2*1024*1024), &input)
		if err == nil && (input.TTL <= 0 || input.TTL > 86400) {
			err = errors.New("ttl_seconds must be between 1 and 86400")
		}
		if err == nil {
			err = d.enqueue(common.EmailDeliveryRequest{Subject: input.Subject, Receiver: input.Receiver, Content: input.Content, ExpiresAt: d.Now().Add(time.Duration(input.TTL) * time.Second)})
		}
		result = "mail queued"
	case "jobs":
		var jobs []model.MailJob
		err = d.DB.Order("created_at desc").Limit(100).Find(&jobs).Error
		result = jobs
	case "attempts":
		if len(args) != 2 {
			return 1
		}
		var attempts []model.MailAttempt
		err = d.DB.Where("job_id = ?", args[1]).Find(&attempts).Error
		result = attempts
	case "run":
		err = d.RunOnce(context.Background())
		result = "mail queue processed"
	case "resolve":
		if len(args) != 3 || args[2] != "delivered" && args[2] != "rejected" {
			fmt.Fprintln(errOut, "resolve requires an attempt ID and a verified delivered/rejected outcome")
			return 1
		}
		var a model.MailAttempt
		err = d.DB.First(&a, "id = ?", args[1]).Error
		if err == nil {
			err = d.settle(a, Result{State: args[2], Code: "operator_confirmed"})
		}
		if err == nil && args[2] == "delivered" {
			err = d.DB.Model(&model.MailJob{}).Where("id = ?", a.JobID).Updates(map[string]any{"state": "delivered", "payload": "", "lease": "", "lease_until": 0, "next_attempt": 0, "last_code": "operator_confirmed"}).Error
		}
		result = "attempt resolved"

	case "reset-period":
		err = d.resetPeriod(args[1:])
		result = "provider-confirmed quota period updated"
	default:
		err = errors.New("unknown mail command")
	}
	if err != nil {
		var safe configurationError
		if errors.As(err, &safe) {
			fmt.Fprintln(errOut, "mail command failed:", safe)
		} else {
			fmt.Fprintln(errOut, "mail command failed; check configuration and database availability")
		}
		return 1
	}
	raw, err := common.Marshal(result)
	if err != nil {
		return 1
	}
	fmt.Fprintln(out, string(raw))
	return 0
}
