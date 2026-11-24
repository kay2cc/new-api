package maildelivery

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
)

// MAIL_ENCRYPTION_KEY is a stable base64-encoded 32-byte key, stored outside the DB.
func encryptionKey() ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(os.Getenv("MAIL_ENCRYPTION_KEY"))
	if err != nil || len(key) != 32 {
		return nil, errors.New("MAIL_ENCRYPTION_KEY must contain a base64-encoded 32-byte key")
	}
	return key, nil
}

func seal(plain []byte, purpose string) (string, error) {
	key, err := encryptionKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, plain, []byte(purpose))), nil
}

func unseal(value, purpose string) ([]byte, error) {
	key, err := encryptionKey()
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(data) < gcm.NonceSize() {
		return nil, errors.New("invalid encrypted mail data")
	}
	plain, err := gcm.Open(nil, data[:gcm.NonceSize()], data[gcm.NonceSize():], []byte(purpose))
	if err != nil {
		return nil, errors.New("mail decryption failed")
	}
	return plain, nil
}
