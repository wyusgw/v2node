package core

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"testing"
	"time"

	panel "github.com/wyusgw/v2node/api/v2board"
	"github.com/wyusgw/v2node/common/crypt"
	"github.com/wyusgw/v2node/core/app/dispatcher"
	_ "github.com/wyusgw/v2node/core/distro/all"
	"github.com/wyusgw/v2node/limiter"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/stats"
	xnet "github.com/xtls/xray-core/common/net"
	xprotocol "github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	xcore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/features/routing"
	xconf "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/dokodemo"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/proxy/wireguard"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/testing/servers/udp"
	"google.golang.org/protobuf/proto"
)

const (
	testUUID = "e5d9b8a1-9d3c-4c2e-9f0f-6b1f2a3c4d5e"
	testUID  = 1
	// Node secret key as a panel would store it.
	testNodeSecret = "EGs4lTSJPmgELx6YiJAmPR2meWi6bY+e9rTdCipSj10="
	// An optional pre-shared key, base64 as the panel stores and publishes it.
	testPreSharedKey    = "UGFuZWxQcmVTaGFyZWRLZXlGb3JUZXN0aW5nMDAwMDA="
	testPreSharedKeyHex = "50616e656c5072655368617265644b6579466f7254657374696e673030303030"
)

// tunnelDest is what the client asks for inside the tunnel. It only has to be
// something the client's netstack will route (127.0.0.0/8 is dropped there as
// martian); the node rewrites it to the echo server.
var tunnelDest = xnet.TCPDestination(xnet.IPAddress([]byte{10, 99, 99, 99}), 9999)

func xorBytes(b []byte) []byte {
	r := make([]byte, len(b))
	for i, v := range b {
		r[i] = v ^ 'c'
	}
	return r
}

// TestWireGuardNodeEndToEnd runs a node the way node/controller.go does —
// v2node's own dispatcher and limiter, inbound added at runtime, users added
// through AddUsers — and checks that traffic reaches the internet and lands on
// the right user's meter.
func TestWireGuardNodeEndToEnd(t *testing.T) {
	// One echo server per network, on the same port, so the node can send both
	// there with a single destination override.
	echoPort := udp.PickPort()
	tcpServer := tcp.Server{MsgProcessor: xorBytes, Port: echoPort}
	dest, err := tcpServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpServer.Close()
	udpServer := udp.Server{MsgProcessor: xorBytes, Port: echoPort}
	if _, err := udpServer.Start(); err != nil {
		t.Fatal(err)
	}
	defer udpServer.Close()

	nodePublicHex := publicKeyOf(t, testNodeSecret)
	serverPort := udp.PickPort()
	nodeInfo := &panel.NodeInfo{
		Id:       1,
		Type:     "wireguard",
		Security: 0,
		Tag:      "wg-node",
		Common: &panel.CommonNode{
			Protocol:     "wireguard",
			ListenIP:     "127.0.0.1",
			ServerPort:   int(serverPort),
			SecretKey:    testNodeSecret,
			PublicKey:    nodePublicHex,
			PreSharedKey: testPreSharedKey,
			Address:      json.RawMessage(`["10.0.0.1/24"]`),
			Mtu:          1420,
		},
	}
	users := []panel.UserInfo{{Id: testUID, Uuid: testUUID}}

	// The limiter has to exist before any traffic: v2node's dispatcher rejects
	// every connection whose tag it cannot find a limiter for.
	limiter.Init()
	nodeLimiter := limiter.AddLimiter(nodeInfo.Type, nodeInfo.Tag, users, map[int]int{})
	defer limiter.DeleteLimiter(nodeInfo.Tag)

	v := newTestCore(t, dest)
	defer v.Close()

	if err := v.AddNode(nodeInfo.Tag, nodeInfo); err != nil {
		t.Fatal("add node: ", err)
	}
	added, err := v.AddUsers(&AddUsersParams{Tag: nodeInfo.Tag, Users: users, NodeInfo: nodeInfo})
	if err != nil {
		t.Fatal("add users: ", err)
	}
	if added != 1 {
		t.Fatalf("added %d users, want 1", added)
	}
	userManager, err := v.GetUserManager(nodeInfo.Tag)
	if err != nil {
		t.Fatal("get user manager: ", err)
	}
	if got := userManager.GetUsersCount(context.Background()); got != 1 {
		t.Errorf("user count = %d, want 1", got)
	}

	// A client built the way a subscription would be: private key derived from
	// the uuid, tunnel address derived from the id.
	clientPort := tcp.PickPort()
	clientUDPPort := udp.PickPort()
	peerAddress, err := wireGuardPeerIPs(mustNetworks(t, nodeInfo.Common), testUID)
	if err != nil {
		t.Fatal(err)
	}
	clientInstance, err := xcore.New(&xcore.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
		Inbound: []*xcore.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xnet.PortList{Range: []*xnet.PortRange{xnet.SinglePortRange(clientPort)}},
				Listen:   xnet.NewIPOrDomain(xnet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress:  xnet.NewIPOrDomain(tunnelDest.Address),
				RewritePort:     uint32(tunnelDest.Port),
				AllowedNetworks: []xnet.Network{xnet.Network_TCP},
			}),
		}, {
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xnet.PortList{Range: []*xnet.PortRange{xnet.SinglePortRange(clientUDPPort)}},
				Listen:   xnet.NewIPOrDomain(xnet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress:  xnet.NewIPOrDomain(tunnelDest.Address),
				RewritePort:     uint32(tunnelDest.Port),
				AllowedNetworks: []xnet.Network{xnet.Network_UDP},
			}),
		}},
		Outbound: []*xcore.OutboundHandlerConfig{{
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
			ProxySettings: serial.ToTypedMessage(&wireguard.DeviceConfig{
				IsClient:    true,
				NoKernelTun: true,
				Endpoint:    []string{peerAddress[0][:len(peerAddress[0])-3]},
				Mtu:         1420,
				// What the panel writes into the subscription. Spelled out
				// rather than reusing wgUserKeyPrefix, so that dropping the
				// prefix on one side alone fails this test.
				SecretKey: hex.EncodeToString(crypt.GenX25519Private([]byte("wg:" + testUUID))),
				Peers: []*wireguard.PeerConfig{{
					Endpoint:  "127.0.0.1:" + serverPort.String(),
					PublicKey: nodePublicHex,
					// The panel puts this in the subscription too; the node has
					// to have set it on the peer or the handshake never lands.
					PreSharedKey: testPreSharedKeyHex,
					AllowedIps:   []string{"0.0.0.0/0", "::0/0"},
				}},
			}),
		}},
	})
	if err != nil {
		t.Fatal("new client: ", err)
	}
	if err := clientInstance.Start(); err != nil {
		t.Fatal("start client: ", err)
	}
	defer clientInstance.Close()

	const tcpPayloadSize = 1024
	conn, err := xnet.DialTCP("tcp", nil, &xnet.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(clientPort)})
	if err != nil {
		t.Fatal("dial: ", err)
	}
	defer conn.Close()
	payload := make([]byte, tcpPayloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal("write: ", err)
	}
	response := make([]byte, tcpPayloadSize)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal("read: ", err)
	}
	if !bytes.Equal(xorBytes(payload), response) {
		t.Error("response does not match payload")
	}
	conn.Close()

	// UDP takes a different path through the node's netstack, so it is worth
	// its own round trip.
	const udpPayloadSize = 512
	udpConn, err := xnet.DialUDP("udp", nil, &xnet.UDPAddr{IP: []byte{127, 0, 0, 1}, Port: int(clientUDPPort)})
	if err != nil {
		t.Fatal("dial udp: ", err)
	}
	defer udpConn.Close()
	udpPayload := make([]byte, udpPayloadSize)
	if _, err := rand.Read(udpPayload); err != nil {
		t.Fatal(err)
	}
	udpConn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := udpConn.Write(udpPayload); err != nil {
		t.Fatal("write udp: ", err)
	}
	udpResponse := make([]byte, udpPayloadSize)
	n, err := udpConn.Read(udpResponse)
	if err != nil {
		t.Fatal("read udp: ", err)
	}
	if !bytes.Equal(xorBytes(udpPayload), udpResponse[:n]) {
		t.Error("udp response does not match payload")
	}
	udpConn.Close()

	// What the node would push to the panel.
	traffic, err := v.GetUserTrafficSlice(nodeInfo.Tag, 0)
	if err != nil {
		t.Fatal("get traffic: ", err)
	}
	if len(traffic) != 1 {
		t.Fatalf("traffic entries = %d, want 1", len(traffic))
	}
	if traffic[0].UID != testUID {
		t.Errorf("traffic UID = %d, want %d", traffic[0].UID, testUID)
	}
	if traffic[0].Upload < tcpPayloadSize+udpPayloadSize {
		t.Errorf("upload = %d, want at least %d", traffic[0].Upload, tcpPayloadSize+udpPayloadSize)
	}
	if traffic[0].Download < tcpPayloadSize+udpPayloadSize {
		t.Errorf("download = %d, want at least %d", traffic[0].Download, tcpPayloadSize+udpPayloadSize)
	}
	t.Logf("reported uid=%d upload=%d download=%d", traffic[0].UID, traffic[0].Upload, traffic[0].Download)

	// Online devices are reported per source address, which inside a tunnel is
	// the peer address — one per user, not the real client IP.
	online, err := nodeLimiter.GetOnlineDevice()
	if err != nil {
		t.Fatal("get online device: ", err)
	}
	if len(*online) != 1 || (*online)[0].UID != testUID {
		t.Errorf("online devices = %v, want one entry for uid %d", *online, testUID)
	} else if want := peerAddress[0][:len(peerAddress[0])-3]; (*online)[0].IP != want {
		t.Errorf("online ip = %q, want %q", (*online)[0].IP, want)
	}

	if err := v.DelUsers(users, nodeInfo.Tag, nodeInfo); err != nil {
		t.Fatal("del users: ", err)
	}
	if got := userManager.GetUsersCount(context.Background()); got != 0 {
		t.Errorf("user count after remove = %d, want 0", got)
	}
	if err := v.DelNode(nodeInfo.Tag); err != nil {
		t.Fatal("del node: ", err)
	}
}

// newTestCore builds a V2Core around the same app stack getCore assembles, with
// an outbound that sends everything to the echo server: the client's netstack
// will not route the loopback address the echo server actually listens on.
func newTestCore(t *testing.T, dest xnet.Destination) *V2Core {
	t.Helper()
	policyConfig, err := (&xconf.PolicyConfig{
		Levels: map[uint32]*xconf.Policy{0: {
			StatsUserUplink:   true,
			StatsUserDownlink: true,
			Handshake:         proto.Uint32(4),
			ConnectionIdle:    proto.Uint32(120),
			UplinkOnly:        proto.Uint32(2),
			DownlinkOnly:      proto.Uint32(4),
			BufferSize:        proto.Int32(128),
		}},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	instance, err := xcore.New(&xcore.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(policyConfig),
		},
		Outbound: []*xcore.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{
				FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				DestinationOverride: &freedom.DestinationOverride{
					Server: &xprotocol.ServerEndpoint{
						Address: xnet.NewIPOrDomain(dest.Address),
						Port:    uint32(dest.Port),
					},
				},
			}),
		}},
	})
	if err != nil {
		t.Fatal("new instance: ", err)
	}
	if err := instance.Start(); err != nil {
		t.Fatal("start instance: ", err)
	}
	return &V2Core{
		Server:     instance,
		users:      &UserMap{uidMap: make(map[string]int)},
		ihm:        instance.GetFeature(inbound.ManagerType()).(inbound.Manager),
		dispatcher: instance.GetFeature(routing.DispatcherType()).(*dispatcher.DefaultDispatcher),
	}
}

func publicKeyOf(t *testing.T, secret string) string {
	t.Helper()
	secretHex, err := xconf.ParseWireGuardKey(secret)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		t.Fatal(err)
	}
	private, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(private.PublicKey().Bytes())
}

func mustNetworks(t *testing.T, common *panel.CommonNode) []wgNetwork {
	t.Helper()
	networks, err := wireGuardNetworks(common)
	if err != nil {
		t.Fatal(err)
	}
	return networks
}

func TestWireGuardPeerAddressing(t *testing.T) {
	networks, err := wireGuardNetworks(&panel.CommonNode{
		Address: json.RawMessage(`["10.0.0.1/16", "fd59:7153:2388:b5fd::1/64"]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		uid  int
		want []string
	}{
		{1, []string{"10.0.0.2/32", "fd59:7153:2388:b5fd::2/128"}},
		{2, []string{"10.0.0.3/32", "fd59:7153:2388:b5fd::3/128"}},
		{300, []string{"10.0.1.45/32", "fd59:7153:2388:b5fd::12d/128"}},
		{65534, []string{"10.0.255.255/32", "fd59:7153:2388:b5fd::ffff/128"}},
	} {
		got, err := wireGuardPeerIPs(networks, c.uid)
		if err != nil {
			t.Fatalf("uid %d: %s", c.uid, err)
		}
		if len(got) != len(c.want) || got[0] != c.want[0] || got[1] != c.want[1] {
			t.Errorf("uid %d = %v, want %v", c.uid, got, c.want)
		}
	}
	// One past the end of the /16 pool.
	if _, err := wireGuardPeerIPs(networks, 65535); err == nil {
		t.Error("expected an error for a uid past the end of the pool")
	}
	// User ids start at 1, so nothing should land on the node's own address —
	// but a node addressed anywhere other than the first address of its pool
	// collides with whichever user maps there.
	if _, err := wireGuardPeerIPs(networks, 0); err == nil {
		t.Error("expected an error for uid 0, which lands on the node address")
	}
	single, err := wireGuardNetworks(&panel.CommonNode{Address: json.RawMessage(`"10.0.0.3/24"`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wireGuardPeerIPs(single, 2); err == nil {
		t.Error("expected an error for a uid landing on the node address")
	}
	// A bare address keeps a pool worth using, and no address at all still works.
	bare, err := wireGuardNetworks(&panel.CommonNode{Address: json.RawMessage(`"10.0.0.1"`)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := wireGuardPeerIPs(bare, 70000); err != nil || got[0] != "10.1.17.113/32" {
		t.Errorf("bare address: %v %v", got, err)
	}
	empty, err := wireGuardNetworks(&panel.CommonNode{})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 1 || empty[0].server.String() != "10.0.0.1" {
		t.Errorf("default address = %v", empty)
	}
}

// The three vectors below were computed outside Go, straight from the panel's
// formulas, and are what actually pins the two sides together. A change to
// either implementation that does not keep them byte-identical breaks every
// handshake, so the test hardcodes the numbers rather than deriving them.
const (
	// base64(clamp(sha256("wg:" + uuid))) — what the subscription hands the client.
	panelUserSecret = "8FjQlUwdvOaVhdKDKXiwjnrcHzlEdJpOieMEhtw6kUg="
	// its public half, which the node registers as the peer.
	panelUserPublicHex = "9bc5005a0fc7ad7ebcc9dd4ea9dedde35f9efbe6f2236d83275d4373a2e7a108"
	// base64(clamp(sha256("wgnode:" + token + ":" + node id))) for node 7.
	panelToken      = "test-token"
	panelNodeSecret = "IOdHs82T7KIXzyqDnQPKF4sdpbmDV88YKRP5hMQjlWs="
	panelNodePublic = "k46nSCjwDbVnB5ZlJRDIYuup6CwYf4j92bVTIxyAXWU="
)

func TestWireGuardUserKeyMatchesPanel(t *testing.T) {
	if got := base64.StdEncoding.EncodeToString(crypt.GenX25519Private([]byte("wg:" + testUUID))); got != panelUserSecret {
		t.Errorf("user secret key = %s, want %s", got, panelUserSecret)
	}
	got, err := wireGuardPublicKey(testUUID)
	if err != nil {
		t.Fatal(err)
	}
	if got != panelUserPublicHex {
		t.Errorf("user public key = %s, want %s", got, panelUserPublicHex)
	}
}

func TestWireGuardNodeKeyMatchesPanel(t *testing.T) {
	if got := wireGuardNodeSecretKey(panelToken, 7); got != panelNodeSecret {
		t.Errorf("node secret key = %s, want %s", got, panelNodeSecret)
	}
	// Without the node id in the seed every node on a panel shares one key.
	if wireGuardNodeSecretKey(panelToken, 8) == panelNodeSecret {
		t.Error("node 8 derives node 7's key")
	}
	if wireGuardNodeSecretKey("other-token", 7) == panelNodeSecret {
		t.Error("a different token derives the same key")
	}
}

func TestWireGuardKeyPairCheck(t *testing.T) {
	if err := checkWireGuardKeyPair(panelNodeSecret, panelNodePublic); err != nil {
		t.Errorf("matching pair rejected: %s", err)
	}
	// The same key in the encoding xray writes.
	raw, err := base64.StdEncoding.DecodeString(panelNodePublic)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkWireGuardKeyPair(panelNodeSecret, hex.EncodeToString(raw)); err != nil {
		t.Errorf("hex public key rejected: %s", err)
	}
	// An admin who filled in a public key from some other key pair.
	if err := checkWireGuardKeyPair(testNodeSecret, panelNodePublic); err == nil {
		t.Error("expected a mismatch error")
	}
	if err := checkWireGuardKeyPair(panelNodeSecret, "not-a-key"); err == nil {
		t.Error("expected an error for an unparsable public key")
	}
}

// TestBuildWireGuardDerivesNodeKey covers the case the panel leaves the private
// key empty for: the node has to arrive at the same key pair on its own, and
// refuse to run if it does not.
func TestBuildWireGuardDerivesNodeKey(t *testing.T) {
	nodeInfo := &panel.NodeInfo{
		Id:    7,
		Type:  "wireguard",
		Token: panelToken,
		Common: &panel.CommonNode{
			ServerPort: 51820,
			PublicKey:  panelNodePublic,
			Address:    json.RawMessage(`["10.0.0.1/24"]`),
			Mtu:        1420,
			Reserved:   []int{1, 2, 3},
		},
	}
	in := &xconf.InboundDetourConfig{}
	if err := buildWireGuard(nodeInfo, in); err != nil {
		t.Fatal(err)
	}
	settings := &xconf.WireGuardConfig{}
	if err := json.Unmarshal(*in.Settings, settings); err != nil {
		t.Fatal(err)
	}
	if settings.SecretKey != panelNodeSecret {
		t.Errorf("secret key = %s, want %s", settings.SecretKey, panelNodeSecret)
	}
	if !bytes.Equal(settings.Reserved, []byte{1, 2, 3}) {
		t.Errorf("reserved = %v, want [1 2 3]", settings.Reserved)
	}

	// A public key the node cannot possibly serve: every user would be handed
	// it in their subscription and none of them could connect.
	nodeInfo.Common.PublicKey = publicKeyOf(t, testNodeSecret)
	if err := buildWireGuard(nodeInfo, &xconf.InboundDetourConfig{}); err == nil {
		t.Error("expected buildWireGuard to refuse a key pair the panel did not publish")
	}
}

func TestWireGuardPreSharedKey(t *testing.T) {
	// The panel stores and publishes base64; wireguard-go's UAPI takes hex.
	got, err := wireGuardPreSharedKey(testPreSharedKey)
	if err != nil {
		t.Fatal(err)
	}
	if got != testPreSharedKeyHex {
		t.Errorf("pre-shared key = %s, want %s", got, testPreSharedKeyHex)
	}
	// Not set is the default and must stay empty, not "the empty key".
	if got, err := wireGuardPreSharedKey(""); err != nil || got != "" {
		t.Errorf("empty pre-shared key = %q %v", got, err)
	}
	if _, err := wireGuardPreSharedKey("not-a-key"); err == nil {
		t.Error("expected an error for an unparsable pre-shared key")
	}
	// A node whose pre-shared key cannot be parsed must not come up: every
	// user carries that key in their subscription.
	nodeInfo := &panel.NodeInfo{
		Id:    7,
		Type:  "wireguard",
		Token: panelToken,
		Common: &panel.CommonNode{
			ServerPort:   51820,
			Address:      json.RawMessage(`["10.0.0.1/24"]`),
			Mtu:          1420,
			PreSharedKey: "not-a-key",
		},
	}
	if err := buildWireGuard(nodeInfo, &xconf.InboundDetourConfig{}); err == nil {
		t.Error("expected buildWireGuard to refuse an unparsable pre-shared key")
	}
}

func TestWireGuardReservedField(t *testing.T) {
	if got, err := wireGuardReserved(nil); err != nil || got != nil {
		t.Errorf("no reserved field = %v %v", got, err)
	}
	if got, err := wireGuardReserved([]int{0, 0, 0}); err != nil || !bytes.Equal(got, []byte{0, 0, 0}) {
		t.Errorf("reserved [0 0 0] = %v %v", got, err)
	}
	if _, err := wireGuardReserved([]int{0, 0}); err == nil {
		t.Error("expected an error for a 2-byte reserved field")
	}
	if _, err := wireGuardReserved([]int{0, 0, 256}); err == nil {
		t.Error("expected an error for a byte out of range")
	}
}
