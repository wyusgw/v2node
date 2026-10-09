package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyusgw/v2node/api/v2board"
)

func (c *Controller) renewCertTask(_ context.Context) error {
	cert := c.info.Common.CertInfo
	// requestCert keeps the node up on the old files when obtaining a
	// replacement fails, so retry that here before trying a plain renewal.
	if !certUsable(cert.CertFile, cert.KeyFile, cert.CertDomain, true) {
		if err := obtainCert(cert); err != nil {
			log.WithField("tag", c.tag).Info("obtain cert error: ", err)
			c.setFault("cert", "obtain cert failed: "+err.Error())
			return nil
		}
		c.clearFault("cert")
		return nil
	}
	l, err := NewLego(cert)
	if err != nil {
		log.WithField("tag", c.tag).Info("new lego error: ", err)
		c.setFault("cert", "init cert client failed: "+err.Error())
		return nil
	}
	err = l.RenewCert()
	if err != nil {
		log.WithField("tag", c.tag).Info("renew cert error: ", err)
		c.setFault("cert", "renew cert failed: "+err.Error())
		return nil
	}
	c.clearFault("cert")
	return nil
}

func (c *Controller) requestCert() error {
	cert := c.info.Common.CertInfo
	switch cert.CertMode {
	case "none", "":
	case "file":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
	case "dns", "http":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		// Existing files are only reused when they still match the panel's
		// settings: a changed domain, or a self-signed cert left over from
		// the self/remote modes, gets a fresh one from the CA.
		if certUsable(cert.CertFile, cert.KeyFile, cert.CertDomain, true) {
			return nil
		}
		if err := obtainCert(cert); err != nil {
			if !certUsable(cert.CertFile, cert.KeyFile, "", false) {
				return err
			}
			log.WithField("tag", c.tag).Error(err, ", keep using the existing cert and retry on the next renew check")
		}
	case "self":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		if certUsable(cert.CertFile, cert.KeyFile, cert.CertDomain, false) {
			return nil
		}
		err := generateSelfSslCertificate(
			cert.CertDomain,
			cert.CertFile,
			cert.KeyFile)
		if err != nil {
			return fmt.Errorf("generate self cert error: %s", err)
		}
	case "remote":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		if cert.TlsCert == "" || cert.TlsKey == "" {
			if certUsable(cert.CertFile, cert.KeyFile, "", false) {
				return nil
			}
			return fmt.Errorf("tls cert or tls key not exist")
		}
		// The panel's cert replaces whatever is on disk whenever they differ,
		// so a cert regenerated on the panel reaches the node on its next reload.
		if fileContentIs(cert.CertFile, cert.TlsCert) && fileContentIs(cert.KeyFile, cert.TlsKey) {
			return nil
		}
		if err := os.WriteFile(cert.CertFile, []byte(cert.TlsCert), 0644); err != nil {
			return fmt.Errorf("write remote cert error: %s", err)
		}
		if err := os.WriteFile(cert.KeyFile, []byte(cert.TlsKey), 0600); err != nil {
			return fmt.Errorf("write remote key error: %s", err)
		}

	default:
		return fmt.Errorf("unsupported certmode: %s", cert.CertMode)
	}
	return nil
}

func obtainCert(cert *panel.CertInfo) error {
	l, err := NewLego(cert)
	if err != nil {
		return fmt.Errorf("create lego object error: %s", err)
	}
	if err = l.CreateCert(); err != nil {
		return fmt.Errorf("create lego cert error: %s", err)
	}
	return nil
}

// certUsable reports whether certPath and keyPath hold a matching cert/key
// pair that covers domain (skipped when empty). requireCA additionally
// rejects self-signed certs.
func certUsable(certPath, keyPath, domain string, requireCA bool) bool {
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return false
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return false
	}
	if domain != "" && leaf.VerifyHostname(domain) != nil {
		return false
	}
	if requireCA && bytes.Equal(leaf.RawIssuer, leaf.RawSubject) &&
		leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature) == nil {
		return false
	}
	return true
}

func fileContentIs(path, content string) bool {
	data, err := os.ReadFile(path)
	return err == nil && string(data) == content
}

func generateSelfSslCertificate(domain, certPath, keyPath string) error {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := &x509.Certificate{
		Version:      3,
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			CommonName: domain,
		},
		DNSNames:              []string{domain},
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(30, 0, 0),
	}
	cert, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return err
	}
	err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	}), 0644)
	if err != nil {
		return err
	}
	return os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}), 0600)
}
