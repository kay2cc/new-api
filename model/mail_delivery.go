package model

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// MailPayload avoids the 64 KiB MySQL TEXT ceiling for encrypted HTML bodies.
type MailPayload string

func (MailPayload) GormDataType() string { return "text" }
func (MailPayload) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db.Dialector.Name() == "mysql" {
		return "longtext"
	}
	return "text"
}

// MailAccount owns provider credentials and shared quotas, independently of From addresses.
type MailAccount struct {
	ID            int64  `gorm:"primaryKey"`
	Name          string `gorm:"size:100;uniqueIndex"`
	Provider      string `gorm:"size:40"`
	Secret        string `gorm:"type:text" json:"-"`
	Enabled       bool
	CooldownUntil int64
}

type MailChannel struct {
	ID          int64  `gorm:"primaryKey"`
	AccountID   int64  `gorm:"index"`
	Name        string `gorm:"size:100;uniqueIndex"`
	FromAddress string `gorm:"size:254"`
	FromName    string `gorm:"size:100"`
	Priority    int
	Enabled     bool
}

// MailQuota is an account-wide budget. Unconfirmed calendars must use manual
// mode; provider_balance uses the provider's balance rather than guessing midnight.
type MailQuota struct {
	ID          int64  `gorm:"primaryKey"`
	AccountID   int64  `gorm:"uniqueIndex:idx_mail_account_budget"`
	Name        string `gorm:"size:40;uniqueIndex:idx_mail_account_budget"`
	Mode        string `gorm:"size:40"`
	Limit       int64
	Capacity    int64
	Used        int64
	Reserved    int64
	Timezone    string `gorm:"size:80"`
	Period      string `gorm:"size:100"`
	PeriodEnd   int64
	Observed    int64
	Initialized bool
}

type MailJob struct {
	ID          string      `gorm:"size:36;primaryKey"`
	Payload     MailPayload `json:"-"`
	State       string      `gorm:"size:30;index:idx_mail_due"`
	NextAttempt int64       `gorm:"index:idx_mail_due"`
	ExpiresAt   int64       `gorm:"index"`
	ProcessID   string      `gorm:"size:36"`
	ValidityTag string      `gorm:"size:100"`
	Lease       string      `gorm:"size:36"`
	LeaseUntil  int64
	Attempts    int
	CreatedAt   int64
	LastCode    string `gorm:"size:80"`
}

type MailAttempt struct {
	ID          string `gorm:"size:36;primaryKey"`
	JobID       string `gorm:"size:36;index"`
	FromAddress string `gorm:"size:254"`
	FromName    string `gorm:"size:100"`
	ChannelID   int64
	AccountID   int64  `gorm:"index"`
	State       string `gorm:"size:30;index"`
	ProviderID  string `gorm:"size:254"`
	Code        string `gorm:"size:80"`
	Units       int64
	CreatedAt   int64
	UpdatedAt   int64
}

// Reservations retain their original period so late delivery events never debit a new one.
type MailReservation struct {
	ID        int64  `gorm:"primaryKey"`
	AttemptID string `gorm:"size:36;uniqueIndex:idx_mail_reservation"`
	QuotaID   int64  `gorm:"uniqueIndex:idx_mail_reservation"`
	Period    string `gorm:"size:100"`
	Units     int64
}

func MigrateMailDelivery(db *gorm.DB) error {
	return db.AutoMigrate(&MailAccount{}, &MailChannel{}, &MailQuota{}, &MailJob{}, &MailAttempt{}, &MailReservation{})
}
