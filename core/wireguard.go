package core

import (
	"bytes"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/crypt"
)

// WireGuard addresses its peers inside the tunnel, so every user needs a key
// pair and a stable tunnel address of their own. The panel hands out nothing
// but a uuid and a numeric id, so both are derived from those here, and
// whatever writes the client config has to derive them exactly the same way:
//
//	private key = sha256("wg:" + uuid), clamped for x25519
//	address     = <pool network> + id + wgPeerOffset
//
// Both halves have to stay byte-for-byte identical to the panel's
// Helper::wireguardPrivateKey() and its id + 1 offset. Get the key wrong and
// the handshake fails; get the offset wrong and the tunnel comes up but every
// packet is dropped by AllowedIPs.
//
// User ids start at 1, so offset 1 puts the first user on .2 and leaves .1,
// where an "10.0.0.1/8" style node address puts the node itself, alone.
const wgPeerOffset = 1

// wgUserKeyPrefix domain-separates the uuid: the same uuid is also a vmess id
// and a trojan password, so the panel hashes it with a prefix here.
const wgUserKeyPrefix = "wg:"

// wgNodeKeyPrefix domain-separates the node's own key seed the same way.
const wgNodeKeyPrefix = "wgnode:"

// Address prefixes to assume when the panel sends a bare address. /8 leaves
// room for every id a panel is realistically going to hand out.
const (
	wgDefaultV4Bits = 8
	wgDefaultV6Bits = 64
)

var wgDefaultAddress = []string{"10.0.0.1/8"}

// wgNetwork is one tunnel network the node serves: the address the node itself
// answers on, and the prefix its peers are numbered inside.
type wgNetwork struct {
	server netip.Addr
	pool   netip.Prefix
}

func wireGuardSecretKey(c *panel.CommonNode) string {
	if c.SecretKey != "" {
		return c.SecretKey
	}
	return c.PrivateKey
}

func wireGuardNetworks(c *panel.CommonNode) ([]wgNetwork, error) {
	addresses, err := wireGuardAddresses(c.Address)
	if err != nil {
		return nil, err
	}
	networks := make([]wgNetwork, 0, len(addresses))
	for _, address := range addresses {
		network, err := parseWireGuardNetwork(address)
		if err != nil {
			return nil, err
		}
		networks = append(networks, network)
	}
	return networks, nil
}

// wireGuardAddresses accepts either a list or a single address, so a panel
// sending "10.0.0.1/8" rather than ["10.0.0.1/8"] still works.
func wireGuardAddresses(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return wgDefaultAddress, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		if len(list) == 0 {
			return wgDefaultAddress, nil
		}
		return list, nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, fmt.Errorf("unmarshal wireguard address error: %s", err)
	}
	if single == "" {
		return wgDefaultAddress, nil
	}
	return []string{single}, nil
}

func parseWireGuardNetwork(address string) (wgNetwork, error) {
	if strings.Contains(address, "/") {
		prefix, err := netip.ParsePrefix(address)
		if err != nil {
			return wgNetwork{}, fmt.Errorf("the wireguard address %s is not vail: %s", address, err)
		}
		return wgNetwork{server: prefix.Addr(), pool: prefix.Masked()}, nil
	}
	addr, err := netip.ParseAddr(address)
	if err != nil {
		return wgNetwork{}, fmt.Errorf("the wireguard address %s is not vail: %s", address, err)
	}
	addr = addr.Unmap().WithZone("")
	bits := wgDefaultV4Bits
	if addr.Is6() {
		bits = wgDefaultV6Bits
	}
	return wgNetwork{server: addr, pool: netip.PrefixFrom(addr, bits).Masked()}, nil
}

// wireGuardPublicKey derives a peer's public key from its uuid. Only the public
// half is ever needed here: the device authenticates peers by public key, and
// the private half only has to exist on the client.
func wireGuardPublicKey(uuid string) (string, error) {
	private, err := ecdh.X25519().NewPrivateKey(crypt.GenX25519Private([]byte(wgUserKeyPrefix + uuid)))
	if err != nil {
		return "", fmt.Errorf("derive wireguard key error: %s", err)
	}
	// Hex, not base64: the peer account is parsed with wireguard.ParseKey.
	return hex.EncodeToString(private.PublicKey().Bytes()), nil
}

// wireGuardNodeSecretKey derives the node's own private key from the panel
// token and the node id, exactly as the panel's
// Helper::wireguardNodePrivateKey() does. That is what lets a node run without
// the panel ever storing or sending a private key: both sides compute the same
// one, and the panel publishes the matching public key in every subscription.
//
// The node id must stay in the seed, or every WireGuard node on a panel would
// share a single key pair.
func wireGuardNodeSecretKey(token string, nodeID int) string {
	seed := fmt.Sprintf("%s%s:%d", wgNodeKeyPrefix, token, nodeID)
	// Base64 like the panel writes it; xray parses either encoding.
	return base64.StdEncoding.EncodeToString(crypt.GenX25519Private([]byte(seed)))
}

// wgKeyEncodings covers what a 32-byte key can arrive as. The panel sends
// base64, xray writes hex, and both show up in hand-filled admin fields.
var wgKeyEncodings = []*base64.Encoding{
	base64.StdEncoding,
	base64.RawStdEncoding,
	base64.URLEncoding,
	base64.RawURLEncoding,
}

func wireGuardKeyBytes(key string) ([]byte, error) {
	key = strings.TrimSpace(key)
	if len(key) == 64 {
		if raw, err := hex.DecodeString(key); err == nil {
			return raw, nil
		}
	}
	for _, encoding := range wgKeyEncodings {
		raw, err := encoding.DecodeString(key)
		if err != nil {
			continue
		}
		if len(raw) != 32 {
			return nil, fmt.Errorf("a wireguard key is 32 bytes, this one is %d", len(raw))
		}
		return raw, nil
	}
	// Never echo the key itself: this also runs on private keys.
	return nil, fmt.Errorf("the wireguard key is neither hex nor base64")
}

// checkWireGuardKeyPair refuses to start a node whose key does not match the
// public key the panel published. The panel writes that public key into every
// subscription, so a mismatch means no user can ever handshake, and WireGuard
// answers a bad handshake with silence — nothing in any log would say why.
func checkWireGuardKeyPair(secretKey, publicKey string) error {
	secret, err := wireGuardKeyBytes(secretKey)
	if err != nil {
		return fmt.Errorf("the wireguard secret key is not vail: %s", err)
	}
	want, err := wireGuardKeyBytes(publicKey)
	if err != nil {
		return fmt.Errorf("the wireguard public key is not vail: %s", err)
	}
	private, err := ecdh.X25519().NewPrivateKey(secret)
	if err != nil {
		return fmt.Errorf("derive wireguard key error: %s", err)
	}
	got := private.PublicKey().Bytes()
	if !bytes.Equal(got, want) {
		return fmt.Errorf("this node's wireguard public key is %s but the panel published %s, no user could connect",
			base64.StdEncoding.EncodeToString(got), base64.StdEncoding.EncodeToString(want))
	}
	return nil
}

// wireGuardPreSharedKey converts the panel's optional pre-shared key into the
// hex form wireguard-go's UAPI takes, the same encoding peer public keys use.
// An empty key means the node runs without one, which is the default.
//
// It has to reach every peer: the panel writes the key into each user's
// subscription, so a node that dropped it would answer every one of its own
// users with a failed handshake and nothing in the log.
func wireGuardPreSharedKey(key string) (string, error) {
	if key == "" {
		return "", nil
	}
	raw, err := wireGuardKeyBytes(key)
	if err != nil {
		return "", fmt.Errorf("the wireguard pre-shared key is not vail: %s", err)
	}
	return hex.EncodeToString(raw), nil
}

// wireGuardReserved validates WireGuard's 3-byte reserved field. The panel
// sends it as [0,0,0] or leaves it out; xray takes 0 or 3 bytes and nothing
// else.
func wireGuardReserved(reserved []int) ([]byte, error) {
	if len(reserved) == 0 {
		return nil, nil
	}
	if len(reserved) != 3 {
		return nil, fmt.Errorf("the wireguard reserved field takes 3 bytes, got %d", len(reserved))
	}
	out := make([]byte, 0, 3)
	for _, b := range reserved {
		if b < 0 || b > 255 {
			return nil, fmt.Errorf("the wireguard reserved byte %d is out of range", b)
		}
		out = append(out, byte(b))
	}
	return out, nil
}

// wireGuardPeerIPs returns the peer's allowed IPs, one host route per tunnel
// network. They are what the node routes replies by and what it identifies the
// user by, so they must not overlap between users.
func wireGuardPeerIPs(networks []wgNetwork, uid int) ([]string, error) {
	if uid < 0 {
		return nil, fmt.Errorf("the user id %d is not vail", uid)
	}
	offset := uint64(uid) + wgPeerOffset
	ips := make([]string, 0, len(networks))
	for _, network := range networks {
		hostBits := network.pool.Addr().BitLen() - network.pool.Bits()
		addr := addWireGuardOffset(network.pool.Addr(), offset)
		if (hostBits < 64 && offset >= uint64(1)<<uint(hostBits)) || !network.pool.Contains(addr) {
			return nil, fmt.Errorf("the user id %d does not fit in %s, use a shorter prefix", uid, network.pool)
		}
		if addr == network.server {
			return nil, fmt.Errorf("the user id %d lands on the node address %s", uid, network.server)
		}
		ips = append(ips, netip.PrefixFrom(addr, addr.BitLen()).String())
	}
	return ips, nil
}

func addWireGuardOffset(base netip.Addr, offset uint64) netip.Addr {
	if base.Is4() {
		b := base.As4()
		binary.BigEndian.PutUint32(b[:], binary.BigEndian.Uint32(b[:])+uint32(offset))
		return netip.AddrFrom4(b)
	}
	b := base.As16()
	high := binary.BigEndian.Uint64(b[:8])
	low := binary.BigEndian.Uint64(b[8:])
	if low+offset < low {
		high++
	}
	binary.BigEndian.PutUint64(b[:8], high)
	binary.BigEndian.PutUint64(b[8:], low+offset)
	return netip.AddrFrom16(b)
}
