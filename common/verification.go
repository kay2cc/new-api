package common

import (
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type verificationValue struct {
	code        string
	time        time.Time
	deliveryTag string
}

const (
	EmailVerificationPurpose = "v"
	PasswordResetPurpose     = "r"
)

var verificationMutex sync.Mutex
var verificationMap map[string]verificationValue
var verificationMapMaxSize = 10
var VerificationValidMinutes = 10

func GenerateVerificationCode(length int) string {
	code := uuid.New().String()
	code = strings.Replace(code, "-", "", -1)
	if length == 0 {
		return code
	}
	return code[:length]
}

func RegisterVerificationCodeWithKey(key string, code string, purpose string) {
	verificationMutex.Lock()
	defer verificationMutex.Unlock()
	verificationMap[purpose+key] = verificationValue{
		code:        code,
		deliveryTag: uuid.NewString(),
		time:        time.Now(),
	}
	if len(verificationMap) > verificationMapMaxSize {
		removeExpiredPairs()
	}
}

func VerifyCodeWithKey(key string, code string, purpose string) bool {
	verificationMutex.Lock()
	defer verificationMutex.Unlock()
	value, okay := verificationMap[purpose+key]
	now := time.Now()
	if !okay || int(now.Sub(value.time).Seconds()) >= VerificationValidMinutes*60 {
		return false
	}
	return code == value.code
}

func DeleteKey(key string, purpose string) {
	verificationMutex.Lock()
	defer verificationMutex.Unlock()
	delete(verificationMap, purpose+key)
}

// no lock inside, so the caller must lock the verificationMap before calling!
func removeExpiredPairs() {
	now := time.Now()
	for key := range verificationMap {
		if int(now.Sub(verificationMap[key].time).Seconds()) >= VerificationValidMinutes*60 {
			delete(verificationMap, key)
		}
	}
}

func init() {
	verificationMutex.Lock()
	defer verificationMutex.Unlock()
	verificationMap = make(map[string]verificationValue)
}

// SendVerificationEmail binds a queued task to the current in-memory challenge.
// Replacing/consuming the code or restarting the owning process cancels retries.
func SendVerificationEmail(subject, receiver, content, purpose, code string) error {
	verificationMutex.Lock()
	value, ok := verificationMap[purpose+receiver]
	verificationMutex.Unlock()
	if !ok || value.code != code || !time.Now().Before(value.time.Add(time.Duration(VerificationValidMinutes)*time.Minute)) {
		return NewMessage("Invalid parameters")
	}
	return SendEmailRequest(EmailDeliveryRequest{Subject: subject, Receiver: receiver, Content: content,
		ExpiresAt: value.time.Add(time.Duration(VerificationValidMinutes) * time.Minute), ValidityTag: value.deliveryTag})
}

func IsEmailVerificationCurrent(tag string) bool {
	verificationMutex.Lock()
	defer verificationMutex.Unlock()
	for _, value := range verificationMap {
		if value.deliveryTag == tag && time.Now().Before(value.time.Add(time.Duration(VerificationValidMinutes)*time.Minute)) {
			return true
		}
	}
	return false
}
