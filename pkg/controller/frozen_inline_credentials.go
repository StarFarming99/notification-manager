package controller

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// Inline Notification/Verify API credentials have no CR to reference. Their
// accepted values are AEAD-encrypted using a separately mounted keyring. The
// keyring is never stored in the spool; previous keys must stay installed until
// their frozen queues are drained or explicitly reconciled.
func frozenCredentialCipher(keyID string) (cipher.AEAD, error) {
	var keys map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("NM_FROZEN_CREDENTIAL_KEYS")), &keys); err != nil {
		return nil, errors.New("frozen credential keyring unavailable")
	}
	raw, err := base64.StdEncoding.DecodeString(keys[keyID])
	if err != nil || len(raw) != 32 {
		return nil, errors.New("frozen credential key unavailable")
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func inlineAAD(path []string, keyID string) []byte {
	raw, _ := json.Marshal(struct {
		Path  []string
		KeyID string
	}{path, keyID})
	return raw
}
func encryptInlineCredential(value string, path []string) (CredentialReference, error) {
	id := os.Getenv("NM_FROZEN_CREDENTIAL_ACTIVE_KEY")
	if id == "" || len(id) > 128 {
		return CredentialReference{}, errors.New("inline credentials require a mounted frozen credential keyring")
	}
	aead, err := frozenCredentialCipher(id)
	if err != nil {
		return CredentialReference{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return CredentialReference{}, err
	}
	encrypted := aead.Seal(nonce, nonce, []byte(value), inlineAAD(path, id))
	return CredentialReference{Kind: "encrypted_inline", TargetPath: append([]string(nil), path...), KeyID: id, Ciphertext: base64.StdEncoding.EncodeToString(encrypted)}, nil
}
func decryptInlineCredential(ref CredentialReference) (string, error) {
	aead, err := frozenCredentialCipher(ref.KeyID)
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(ref.Ciphertext)
	if err != nil || len(raw) < aead.NonceSize() {
		return "", errors.New("frozen inline credential invalid")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], inlineAAD(ref.TargetPath, ref.KeyID))
	if err != nil {
		return "", errors.New("frozen inline credential authentication failed")
	}
	return string(plain), nil
}
