package datastore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/scrypt"
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
	cipher cipher.AEAD
	encKey [32]byte
	idKey  [32]byte
}

// NewCryptConfig derives signing and fingerprint keys from a raw 32-byte encryption key.
func NewCryptConfig(encKey [32]byte) (*CryptConfig, error) {
	block, err := aes.NewCipher(encKey[:])
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	var idKey [32]byte
	pbkdf2DeriveFull(encKey[:], idKeySalt, 10, idKey[:])

	return &CryptConfig{
		encKey: encKey,
		idKey:  idKey,
		cipher: aead,
	}, nil
}

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

type KeyConfig struct {
	Kdf         *KeyDerivationConfig `json:"kdf"`
	Created     string               `json:"created"`
	Modified    string               `json:"modified"`
	Data        []byte               `json:"data"`
	Fingerprint string               `json:"fingerprint,omitempty"`
	Hint        string               `json:"hint,omitempty"`
}

type KeyDerivationConfig struct {
	Scrypt *ScryptConfig `json:"Scrypt,omitempty"`
	PBKDF2 *PBKDF2Config `json:"PBKDF2,omitempty"`
}

type ScryptConfig struct {
	N    int    `json:"n"`
	R    int    `json:"r"`
	P    int    `json:"p"`
	Salt []byte `json:"salt"`
}

type PBKDF2Config struct {
	Iter int    `json:"iter"`
	Salt []byte `json:"salt"`
}

// CreateRandomKey generates a random 32-byte encryption key.
func CreateRandomKey() ([32]byte, error) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return [32]byte{}, fmt.Errorf("generate random key: %w", err)
	}
	return key, nil
}

func GenerateKeyFile(password string) ([]byte, error) {
	encKey, err := CreateRandomKey()
	if err != nil {
		return nil, err
	}

	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}

	kdf := &KeyDerivationConfig{
		Scrypt: &ScryptConfig{N: 65536, R: 8, P: 1, Salt: salt},
	}
	derivedKey, err := deriveKeyFromConfig(kdf, []byte(password))
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(derivedKey[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	iv := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("generate iv: %w", err)
	}

	encryptedKey := aead.Seal(nil, iv, encKey[:], nil)
	gcmTagSize := aead.Overhead()

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

	now := time.Now().UTC().Format(time.RFC3339)
	keyConfig := &KeyConfig{
		Kdf:         kdf,
		Created:     now,
		Modified:    now,
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

	if keyConfig.Kdf == nil {
		return nil, fmt.Errorf("key file has no KDF; use LoadKeyFileNoPassword")
	}

	derivedKey, err := deriveKeyFromConfig(keyConfig.Kdf, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("derive key: %w", err)
	}

	block, err := aes.NewCipher(derivedKey[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	aead, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	if len(keyConfig.Data) < 32 {
		return nil, fmt.Errorf("key data too short: %d", len(keyConfig.Data))
	}

	iv := keyConfig.Data[:16]
	tag := keyConfig.Data[16:32]
	ciphertext := keyConfig.Data[32:]

	nonce := iv

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

func LoadKeyFileNoPassword(data []byte) (*CryptConfig, error) {
	var keyConfig KeyConfig
	if err := json.Unmarshal(data, &keyConfig); err != nil {
		return nil, fmt.Errorf("parse key file: %w", err)
	}

	if keyConfig.Kdf != nil {
		return nil, fmt.Errorf("key file requires password")
	}

	if len(keyConfig.Data) != 32 {
		return nil, fmt.Errorf("invalid key length for none kdf: %d", len(keyConfig.Data))
	}

	var encKey [32]byte
	copy(encKey[:], keyConfig.Data)

	return NewCryptConfig(encKey)
}

func deriveKeyFromConfig(kdf *KeyDerivationConfig, password []byte) ([32]byte, error) {
	switch {
	case kdf.PBKDF2 != nil:
		iter := kdf.PBKDF2.Iter
		if iter == 0 {
			iter = 65535
		}
		var key [32]byte
		pbkdf2DeriveFull(password, kdf.PBKDF2.Salt, iter, key[:])
		return key, nil
	case kdf.Scrypt != nil:
		s := kdf.Scrypt
		derived, err := scrypt.Key(password, s.Salt, s.N, s.R, s.P, 32)
		if err != nil {
			return [32]byte{}, fmt.Errorf("scrypt: %w", err)
		}
		return [32]byte(derived), nil
	default:
		return [32]byte{}, fmt.Errorf("unsupported KDF")
	}
}

func pbkdf2DeriveFull(password, salt []byte, iterations int, out []byte) {
	block1 := make([]byte, 0, len(salt)+4)
	block1 = append(block1, salt...)
	block1 = append(block1, 0, 0, 0, 1)
	key := hmac.New(sha256.New, password)
	key.Write(block1)
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
	return b + 'a' - 10
}

func manifestSignature(manifest *Manifest, cc *CryptConfig) ([32]byte, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal for signing: %w", err)
	}

	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var value map[string]any
	if err := dec.Decode(&value); err != nil {
		return [32]byte{}, fmt.Errorf("decode for signing: %w", err)
	}
	delete(value, "signature")
	delete(value, "unprotected")

	var sb strings.Builder
	if err := writeCanonicalJSON(&sb, value); err != nil {
		return [32]byte{}, fmt.Errorf("canonical json: %w", err)
	}

	return cc.AuthTag([]byte(sb.String())), nil
}

func writeCanonicalJSON(sb *strings.Builder, v any) error {
	switch val := v.(type) {
	case nil:
		return fmt.Errorf("got unexpected null value")
	case bool:
		if val {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case json.Number:
		sb.WriteString(val.String())
	case string:
		writeCanonicalString(sb, val)
	case []any:
		sb.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				sb.WriteByte(',')
			}
			if err := writeCanonicalJSON(sb, item); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeCanonicalString(sb, k)
			sb.WriteByte(':')
			if err := writeCanonicalJSON(sb, val[k]); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	default:
		return fmt.Errorf("unexpected json value type %T", v)
	}
	return nil
}

func writeCanonicalString(sb *strings.Builder, s string) {
	const hexChars = "0123456789abcdef"
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			sb.WriteString(`\"`)
		case c == '\\':
			sb.WriteString(`\\`)
		case c == '\b':
			sb.WriteString(`\b`)
		case c == '\t':
			sb.WriteString(`\t`)
		case c == '\n':
			sb.WriteString(`\n`)
		case c == '\f':
			sb.WriteString(`\f`)
		case c == '\r':
			sb.WriteString(`\r`)
		case c < 0x20:
			sb.WriteString(`\u00`)
			sb.WriteByte(hexChars[c>>4])
			sb.WriteByte(hexChars[c&0xf])
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteByte('"')
}

func SignManifest(manifest *Manifest, cc *CryptConfig) error {
	tag, err := manifestSignature(manifest, cc)
	if err != nil {
		return err
	}
	manifest.Signature = hex.EncodeToString(tag[:])

	unprotected := map[string]any{}
	if len(manifest.Unprotected) > 0 {
		if err := json.Unmarshal(manifest.Unprotected, &unprotected); err != nil {
			return fmt.Errorf("parse unprotected: %w", err)
		}
	}
	fp := cc.Fingerprint()
	unprotected["key-fingerprint"] = FormatFingerprint(fp)
	unprotectedJSON, err := json.Marshal(unprotected)
	if err != nil {
		return fmt.Errorf("marshal unprotected: %w", err)
	}
	manifest.Unprotected = unprotectedJSON

	return nil
}

func VerifyManifestSignature(manifest *Manifest, cc *CryptConfig) error {
	if manifest.Signature == "" {
		return fmt.Errorf("manifest has no signature")
	}

	tag, err := manifestSignature(manifest, cc)
	if err != nil {
		return err
	}
	expectedSig := hex.EncodeToString(tag[:])

	if manifest.Signature != expectedSig {
		return fmt.Errorf("signature mismatch: expected %s, got %s", expectedSig, manifest.Signature)
	}

	return nil
}

// UnprotectedInfo holds the unprotected key info in a manifest.
type UnprotectedInfo struct {
	KeyFingerprint string `json:"key-fingerprint"`
}
