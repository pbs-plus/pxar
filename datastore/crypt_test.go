package datastore

import (
	"encoding/json"
	"testing"
)

func TestCryptConfigKeyDerivation(t *testing.T) {
	key, err := CreateRandomKey()
	if err != nil {
		t.Fatal(err)
	}

	cc, err := NewCryptConfig(key)
	if err != nil {
		t.Fatal(err)
	}

	if cc.Fingerprint() == [32]byte{} {
		t.Error("fingerprint should not be zero")
	}
}

func TestCryptConfigEncryptDecrypt(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	plaintext := []byte("hello encryption world")
	ciphertext, err := cc.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	if len(ciphertext) <= len(plaintext) {
		t.Error("ciphertext should be larger than plaintext due to nonce + tag")
	}

	decrypted, err := cc.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestCryptConfigAuthTag(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	data := []byte("manifest data to sign")
	tag1 := cc.AuthTag(data)
	tag2 := cc.AuthTag(data)

	if tag1 != tag2 {
		t.Error("same data should produce same auth tag")
	}

	differentData := []byte("different manifest data")
	tag3 := cc.AuthTag(differentData)

	if tag1 == tag3 {
		t.Error("different data should produce different auth tag")
	}
}

func TestCryptConfigComputeDigest(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	data := []byte("chunk data")
	digest := cc.ComputeDigest(data)
	plainDigest := PlainChunkDigest(data)

	if digest == plainDigest {
		t.Error("encrypted digest should differ from plain digest")
	}
}

func TestManifestSigning(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	manifest := &Manifest{
		BackupType: "host",
		BackupID:   "test",
		BackupTime: 1234567890,
		Files: []FileInfo{
			{Filename: "root.pxar.didx", Size: 1000, CSum: "abc123"},
		},
		CryptMode: "encrypt",
	}

	err := SignManifest(manifest, cc)
	if err != nil {
		t.Fatal(err)
	}

	if manifest.Signature == "" {
		t.Error("manifest should have signature after signing")
	}

	if manifest.Unprotected == nil {
		t.Error("manifest should have unprotected key-fingerprint")
	}

	var unprotected map[string]interface{}
	if err := json.Unmarshal(manifest.Unprotected, &unprotected); err != nil {
		t.Fatal(err)
	}
	if _, ok := unprotected["key-fingerprint"]; !ok {
		t.Error("unprotected should contain key-fingerprint")
	}

	err = VerifyManifestSignature(manifest, cc)
	if err != nil {
		t.Errorf("signature verification failed: %v", err)
	}
}

func TestManifestTamperDetection(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	manifest := &Manifest{
		BackupType: "host",
		BackupID:   "test",
		BackupTime: 1234567890,
		Files: []FileInfo{
			{Filename: "root.pxar.didx", Size: 1000, CSum: "abc123"},
		},
		CryptMode: "encrypt",
	}

	SignManifest(manifest, cc)

	manifest.BackupID = "tampered"

	err := VerifyManifestSignature(manifest, cc)
	if err == nil {
		t.Error("tampered manifest should fail verification")
	}
}

func TestKeyFileRoundTrip(t *testing.T) {
	password := "test-password-123"

	keyData, err := GenerateKeyFile(password)
	if err != nil {
		t.Fatal(err)
	}

	cc, err := LoadKeyFile(keyData, password)
	if err != nil {
		t.Fatal(err)
	}

	if cc.Fingerprint() == [32]byte{} {
		t.Error("loaded key should have valid fingerprint")
	}

	plaintext := []byte("test encryption with loaded key")
	ciphertext, err := cc.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := cc.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestKeyFileWrongPassword(t *testing.T) {
	password := "correct-password"
	keyData, err := GenerateKeyFile(password)
	if err != nil {
		t.Fatal(err)
	}

	_, err = LoadKeyFile(keyData, "wrong-password")
	if err == nil {
		t.Error("loading key with wrong password should fail")
	}
}

func TestFormatFingerprint(t *testing.T) {
	key, _ := CreateRandomKey()
	cc, _ := NewCryptConfig(key)

	fp := cc.Fingerprint()
	formatted := FormatFingerprint(fp)

	if len(formatted) == 0 {
		t.Error("formatted fingerprint should not be empty")
	}

	hasColon := false
	for _, c := range formatted {
		if c == ':' {
			hasColon = true
			break
		}
	}
	if !hasColon {
		t.Error("formatted fingerprint should contain colons")
	}
}
