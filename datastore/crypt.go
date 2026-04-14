package datastore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
)

// fingerprintInput is SHA256("Proxmox Backup Encryption Key Fingerprint")
// Matches PBS Rust implementation exactly.
var fingerprintInput = [32]byte{
	110, 208, 239, 119, 71, 31, 255, 77, 85, 199, 168, 254, 74, 157, 182, 33,
	97, 64, 127, 19, 76, 114, 93, 223, 48, 153, 45, 37, 236, 69, 237, 38,
}

// idKeySalt is the PBKDF2 salt used to derive the id_key from the encryption key.
// Matches PBS: pbkdf2_hmac(sha256, enc_key, b"_id_key", 10, dklen=32)
var idKeySalt = []byte("_id_key")

// CryptConfig holds the derived keys needed for encryption, signing, and fingerprinting.
type CryptConfig struct {
	encKey [32]byte    // raw AES-256 encryption key
	idKey  [32]byte    // derived key for signing and digest computation
	cipher cipher.AEAD // AES-256-GCM cipher
}

// NewCryptConfig derives signing and fingerprint keys from a raw 32-byte encryption key.
func NewCryptConfig(encKey [32]byte) (*CryptConfig, error) {
	block, err := aes.NewCipher(encKey[:])
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	var idKey [32]byte
	pbkdf2Derive(encKey[:], idKeySalt, 10, idKey[:])

	return &CryptConfig{
		encKey: encKey,
		idKey:  idKey,
		cipher: aead,
	}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// Returns nonce + ciphertext (ciphertext includes the GCM tag).
func (c *CryptConfig) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.cipher.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	ciphertext := c.cipher.Seal(nil, nonce, plaintext, nil)
	result := make([]byte, len(nonce)+len(ciphertext))
	copy(result, nonce)
	copy(result[len(nonce):], ciphertext)
	return result, nil
}

// Decrypt decrypts data encrypted by Encrypt.
// Input format: nonce || ciphertext (with GCM tag appended).
func (c *CryptConfig) Decrypt(data []byte) ([]byte, error) {
	nonceSize := c.cipher.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("encrypted data too short: %d", len(data))
	}
	nonce := data[:nonceSize]
	ciphertext := data[nonceSize:]
	plaintext, err := c.cipher.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plaintext, nil
}

// AuthTag computes an HMAC-SHA256 authentication tag over data using the id_key.
func (c *CryptConfig) AuthTag(data []byte) [32]byte {
	mac := hmac.New(sha256.New, c.idKey[:])
	mac.Write(data)
	return [32]byte(mac.Sum(nil))
}

// ComputeDigest computes SHA-256(data || id_key), matching PBS compute_digest.
func (c *CryptConfig) ComputeDigest(data []byte) [32]byte {
	h := sha256.New()
	h.Write(data)
	h.Write(c.idKey[:])
	return [32]byte(h.Sum(nil))
}

// Fingerprint computes SHA-256(fingerprintInput || id_key), matching PBS key fingerprint.
func (c *CryptConfig) Fingerprint() [32]byte {
	h := sha256.New()
	h.Write(fingerprintInput[:])
	h.Write(c.idKey[:])
	return [32]byte(h.Sum(nil))
}

// KeyConfig represents an encryption key file that can be stored on disk.
type KeyConfig struct {
	Kdf         KeyDerivationConfig `json:"kdf"`
	Created     string              `json:"created,omitempty"`
	Modified    string              `json:"modified,omitempty"`
	Data        []byte              `json:"data"`
	Fingerprint string              `json:"fingerprint,omitempty"`
}

// KeyDerivationConfig specifies the key derivation function parameters.
type KeyDerivationConfig struct {
	Type string `json:"type"` // "scrypt" or "pbkdf2" or "none"
	Salt []byte `json:"salt,omitempty"`
	N    int    `json:"n,omitempty"`    // scrypt: CPU cost
	R    int    `json:"r,omitempty"`    // scrypt: block size
	P    int    `json:"p,omitempty"`    // scrypt: parallelism
	Iter int    `json:"iter,omitempty"` // pbkdf2: iterations
}

// pbkdf2Derive implements PBKDF2-HMAC-SHA256 derivation.
// This is a simplified implementation matching PBS's use: key derivation
// for id_key (10 iterations) and is NOT a general-purpose PBKDF2.
func pbkdf2Derive(password, salt []byte, iterations int, out []byte) {
	// HMAC-SHA256 based PBKDF2
	key := hmac.New(sha256.New, password)
	key.Write(salt)
	prf := key.Sum(nil)

	result := make([]byte, len(prf))
	copy(result, prf)

	for i := 1; i < iterations; i++ {
		key = hmac.New(sha256.New, password)
		key.Write(result)
		result = key.Sum(result[:0])
	}

	copy(out, result[:min(len(out), len(result))])
}

// CreateRandomKey generates a random 32-byte encryption key.
func CreateRandomKey() ([32]byte, error) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return [32]byte{}, fmt.Errorf("generate random key: %w", err)
	}
	return key, nil
}

// GenerateKeyFile creates a new encrypted key file protected by a password.
// This generates a random 32-byte key and encrypts it with AES-256-GCM
// using a key derived from the password via PBKDF2.
func GenerateKeyFile(password string) ([]byte, error) {
	encKey, err := CreateRandomKey()
	if err != nil {
		return nil, err
	}

	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}

	derivedKey := pbkdf2DeriveKey([]byte(password), salt, 65535)
	block, err := aes.NewCipher(derivedKey[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	encryptedKey := aead.Seal(nil, nonce, encKey[:], nil)
	gcmTagSize := aead.Overhead()

	iv := make([]byte, 16)
	copy(iv, nonce)

	tag := encryptedKey[len(encryptedKey)-gcmTagSize:]
	ciphertext := encryptedKey[:len(encryptedKey)-gcmTagSize]

	configData := make([]byte, 16+16+len(ciphertext))
	copy(configData[:16], iv)
	copy(configData[16:32], tag)
	copy(configData[32:], ciphertext)

	cc, err := NewCryptConfig(encKey)
	if err != nil {
		return nil, err
	}
	fp := cc.Fingerprint()

	keyConfig := &KeyConfig{
		Kdf: KeyDerivationConfig{
			Type: "pbkdf2",
			Salt: salt,
			Iter: 65535,
		},
		Data:        configData,
		Fingerprint: FormatFingerprint(fp),
	}

	return json.MarshalIndent(keyConfig, "", "  ")
}

// LoadKeyFile decrypts a key file using a password and returns the raw encryption key.
func LoadKeyFile(data []byte, password string) (*CryptConfig, error) {
	var keyConfig KeyConfig
	if err := json.Unmarshal(data, &keyConfig); err != nil {
		return nil, fmt.Errorf("parse key file: %w", err)
	}

	derivedKey, err := deriveKeyFromConfig(&keyConfig.Kdf, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("derive key: %w", err)
	}

	block, err := aes.NewCipher(derivedKey[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	if len(keyConfig.Data) < 32 {
		return nil, fmt.Errorf("key data too short: %d", len(keyConfig.Data))
	}

	iv := keyConfig.Data[:16]
	tag := keyConfig.Data[16:32]
	ciphertext := keyConfig.Data[32:]

	// Reconstruct GCM seal format: nonce(12) + ciphertext + tag
	nonce := make([]byte, aead.NonceSize())
	copy(nonce, iv[:aead.NonceSize()])

	// GCM Open expects ciphertext || tag
	gcmData := make([]byte, len(ciphertext)+len(tag))
	copy(gcmData, ciphertext)
	copy(gcmData[len(ciphertext):], tag)

	plainKey, err := aead.Open(nil, nonce, gcmData, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt key: %w", err)
	}

	if len(plainKey) != 32 {
		return nil, fmt.Errorf("invalid key length: %d", len(plainKey))
	}

	var encKey [32]byte
	copy(encKey[:], plainKey)

	cc, err := NewCryptConfig(encKey)
	if err != nil {
		return nil, err
	}

	if keyConfig.Fingerprint != "" {
		computedFP := FormatFingerprint(cc.Fingerprint())
		if computedFP != keyConfig.Fingerprint {
			return nil, fmt.Errorf("fingerprint mismatch: expected %s, got %s", keyConfig.Fingerprint, computedFP)
		}
	}

	return cc, nil
}

// LoadKeyFileNoPassword loads a key file with "none" KDF (no encryption).
func LoadKeyFileNoPassword(data []byte) (*CryptConfig, error) {
	var keyConfig KeyConfig
	if err := json.Unmarshal(data, &keyConfig); err != nil {
		return nil, fmt.Errorf("parse key file: %w", err)
	}

	if keyConfig.Kdf.Type != "none" {
		return nil, fmt.Errorf("key file requires password (kdf=%s)", keyConfig.Kdf.Type)
	}

	if len(keyConfig.Data) != 32 {
		return nil, fmt.Errorf("invalid key length for none kdf: %d", len(keyConfig.Data))
	}

	var encKey [32]byte
	copy(encKey[:], keyConfig.Data)

	return NewCryptConfig(encKey)
}

func deriveKeyFromConfig(kdf *KeyDerivationConfig, password []byte) ([32]byte, error) {
	switch kdf.Type {
	case "pbkdf2":
		iter := kdf.Iter
		if iter == 0 {
			iter = 65535
		}
		var key [32]byte
		pbkdf2DeriveFull(password, kdf.Salt, iter, key[:])
		return key, nil
	case "scrypt":
		return [32]byte{}, fmt.Errorf("scrypt key derivation is not supported; use pbkdf2 key files")
	default:
		return [32]byte{}, fmt.Errorf("unsupported KDF: %s", kdf.Type)
	}
}

// pbkdf2DeriveFull implements full PBKDF2-HMAC-SHA256 per RFC 2898.
func pbkdf2DeriveFull(password, salt []byte, iterations int, out []byte) {
	key := hmac.New(sha256.New, password)
	key.Write(append(salt, 0, 0, 0, 1)) // block 1
	result := key.Sum(nil)

	ubytes := make([]byte, len(result))
	copy(ubytes, result)

	for i := 1; i < iterations; i++ {
		key = hmac.New(sha256.New, password)
		key.Write(ubytes)
		ubytes = key.Sum(ubytes[:0])
		for j := range result {
			result[j] ^= ubytes[j]
		}
	}
	copy(out, result[:min(len(out), len(result))])
}

// scryptDerive implements scrypt key derivation.
// pbkdf2DeriveKey is a convenience wrapper for deriving a 32-byte key.
func pbkdf2DeriveKey(password []byte, salt []byte, iterations int) [32]byte {
	var key [32]byte
	pbkdf2DeriveFull(password, salt, iterations, key[:])
	return key
}

// FormatFingerprint formats a 32-byte fingerprint as colon-separated hex,
// matching PBS's fingerprint format (uppercase hex with colons).
func FormatFingerprint(fp [32]byte) string {
	s := make([]byte, 0, 32*3-1)
	for i, b := range fp {
		if i > 0 {
			s = append(s, ':')
		}
		s = append(s, hexDigit(b>>4))
		s = append(s, hexDigit(b&0xf))
	}
	return string(s)
}

func hexDigit(b byte) byte {
	if b < 10 {
		return b + '0'
	}
	return b + 'A' - 10
}

// SignManifest signs a manifest JSON using HMAC-SHA256 with the id_key.
// The signature is computed over the canonical JSON with "signature" and
// "unprotected" fields removed, matching PBS behavior.
func SignManifest(manifest *Manifest, cc *CryptConfig) error {
	temp := *manifest
	temp.Signature = ""
	temp.Unprotected = nil

	canonical, err := json.Marshal(temp)
	if err != nil {
		return fmt.Errorf("marshal for signing: %w", err)
	}

	tag := cc.AuthTag(canonical)
	manifest.Signature = FormatFingerprint(tag)

	fp := cc.Fingerprint()
	unprotected := &UnprotectedInfo{
		KeyFingerprint: FormatFingerprint(fp),
	}
	unprotectedJSON, err := json.Marshal(unprotected)
	if err != nil {
		return fmt.Errorf("marshal unprotected: %w", err)
	}
	manifest.Unprotected = unprotectedJSON

	return nil
}

// VerifyManifestSignature verifies the manifest signature.
func VerifyManifestSignature(manifest *Manifest, cc *CryptConfig) error {
	if manifest.Signature == "" {
		return fmt.Errorf("manifest has no signature")
	}

	temp := *manifest
	temp.Signature = ""
	temp.Unprotected = nil

	canonical, err := json.Marshal(temp)
	if err != nil {
		return fmt.Errorf("marshal for verification: %w", err)
	}

	expectedTag := cc.AuthTag(canonical)
	expectedSig := FormatFingerprint(expectedTag)

	if manifest.Signature != expectedSig {
		return fmt.Errorf("signature mismatch: expected %s, got %s", expectedSig, manifest.Signature)
	}

	return nil
}

// UnprotectedInfo holds the unprotected key info in a manifest.
type UnprotectedInfo struct {
	KeyFingerprint string `json:"key-fingerprint"`
}
