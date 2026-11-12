package node

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"path/filepath"
	"testing"
)

func TestGenerateSelfSslCertificateIsECDSA(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "node.cer")
	keyPath := filepath.Join(dir, "node.key")
	if err := generateSelfSslCertificate("example.com", certPath, keyPath); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := pair.PrivateKey.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		t.Fatalf("want an ECDSA P-256 key, got %T", pair.PrivateKey)
	}
	if !certUsable(certPath, keyPath, "example.com", false) {
		t.Fatal("generated certificate is not usable")
	}
}
