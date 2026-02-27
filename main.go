package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	routingDiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discoveryUtil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const MinecraftPort = "25565"
const ProtocolID = protocol.ID("/p2p-tunnel/tcp/" + MinecraftPort)

type mdnsNotifee struct {
	peerChan chan peer.AddrInfo
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	select {
	case m.peerChan <- pi:
	default:
	}
}

func loadOrGenerateKey(path string) (crypto.PrivKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		return crypto.UnmarshalPrivateKey(b)
	}
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}
	b, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(path, b, 0600)
	return priv, err
}

func main() {
	isHost := flag.Bool("host", false, "Працювати як сервер (Minecraft Server)")
	isClient := flag.Bool("client", false, "Працювати як клієнт (Minecraft Player)")
	rendezvous := flag.String("secret", "", "Унікальний секретний ідентифікатор")
	flag.Parse()

	if *rendezvous == "" {
		log.Fatal("Потрібно вказати -secret")
	}
	if (!*isHost && !*isClient) || (*isHost && *isClient) {
		log.Fatal("Потрібно вказати рівно один прапор: -host або -client")
	}

	keyFile := "client_key.dat"
	if *isHost {
		keyFile = "host_key.dat"
	}

	priv, err := loadOrGenerateKey(keyFile)
	if err != nil {
		log.Fatalf("Помилка ініціалізації криптографічного ключа: %s", err)
	}

	ctx := context.Background()

	node, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic-v1"),
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoNATv2(),
	)
	if err != nil {
		log.Fatalf("Помилка створення вузла: %s", err)
	}
	defer node.Close()

	log.Printf("Локальний Peer ID: %s", node.ID())

	mdnsChan := make(chan peer.AddrInfo, 10)
	mdnsService := mdns.NewMdnsService(node, *rendezvous, &mdnsNotifee{peerChan: mdnsChan})
	if err := mdnsService.Start(); err != nil {
		log.Printf("Помилка ініціалізації локального пошуку: %s", err)
	}

	kDHT, err := dht.New(ctx, node, dht.Mode(dht.ModeAutoServer))
	if err != nil {
		log.Fatal(err)
	}
	if err = kDHT.Bootstrap(ctx); err != nil {
		log.Fatal(err)
	}

	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		node.Connect(ctx, *pi)
	}

	rDiscovery := routingDiscovery.NewRoutingDiscovery(kDHT)

	if *isHost {
		setupHost(ctx, node, rDiscovery, *rendezvous, mdnsChan)
	} else {
		setupClient(ctx, node, rDiscovery, *rendezvous, mdnsChan)
	}
}

func establishSymmetricConnection(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, mdnsPeers <-chan peer.AddrInfo) peer.ID {
	log.Printf("Початок публікації та пошуку за ідентифікатором: %s", secret)
	discoveryUtil.Advertise(ctx, rd, secret)

	var targetPeer peer.ID
	successChan := make(chan peer.ID, 1)
	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	dialPeer := func(p peer.AddrInfo) {
		if p.ID == node.ID() || len(p.Addrs) == 0 {
			return
		}

		go func(pi peer.AddrInfo) {
			log.Printf("Ініціалізація підключення до вузла %s...", pi.ID)
			ctxConn, cancelConn := context.WithTimeout(dialCtx, 7*time.Second)
			defer cancelConn()

			if err := node.Connect(ctxConn, pi); err == nil {
				conns := node.Network().ConnsToPeer(pi.ID)
				if len(conns) > 0 {
					log.Printf("Фактична адреса віддаленого вузла: %s", conns[0].RemoteMultiaddr())
				}

				select {
				case successChan <- pi.ID:
				default:
				}
			}
		}(p)
	}

	go func() {
		for {
			select {
			case <-dialCtx.Done():
				return
			default:
			}
			peerChan, err := rd.FindPeers(dialCtx, secret)
			if err == nil {
				for p := range peerChan {
					dialPeer(p)
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()

	go func() {
		for p := range mdnsPeers {
			select {
			case <-dialCtx.Done():
				return
			default:
				dialPeer(p)
			}
		}
	}()

	select {
	case targetPeer = <-successChan:
		conns := node.Network().ConnsToPeer(targetPeer)
		if len(conns) > 0 {
			log.Printf("ТУНЕЛЬ АКТИВНО. Зв'язок на транспортному рівні з %s встановлено", targetPeer)
		}
		cancel()
	case <-ctx.Done():
	}

	return targetPeer
}

func setupHost(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, mdnsPeers <-chan peer.AddrInfo) {
	node.SetStreamHandler(ProtocolID, func(s network.Stream) {
		localConn, err := net.Dial("tcp", "127.0.0.1:"+MinecraftPort)
		if err != nil {
			log.Printf("Відмова локального підключення до сервера Minecraft: %s", err)
			s.Reset()
			return
		}
		go syncStreams(localConn, s)
	})

	fmt.Printf("\nСЕРВЕР ЗАПУЩЕНО. Очікування клієнта...\n")
	targetPeer := establishSymmetricConnection(ctx, node, rd, secret, mdnsPeers)
	log.Printf("ТУНЕЛЬ АКТИВНО (Peer ID: %s)", targetPeer)
	select {}
}

func setupClient(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, mdnsPeers <-chan peer.AddrInfo) {
	fmt.Printf("\nКЛІЄНТ ЗАПУЩЕНО. Очікування сервера...\n")
	targetPeer := establishSymmetricConnection(ctx, node, rd, secret, mdnsPeers)
	log.Printf("ТУНЕЛЬ АКТИВНО (Peer ID: %s)", targetPeer)

	ln, err := net.Listen("tcp", "127.0.0.1:"+MinecraftPort)
	if err != nil {
		log.Fatalf("Помилка ініціалізації локального порту: %s", err)
	}

	fmt.Printf("Готово. Підключіться до сервера Minecraft за адресою: 127.0.0.1:%s\n", MinecraftPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go func() {
			s, err := node.NewStream(ctx, targetPeer, ProtocolID)
			if err != nil {
				conn.Close()
				return
			}
			syncStreams(conn, s)
		}()
	}
}

func syncStreams(conn net.Conn, stream network.Stream) {
	defer conn.Close()
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { io.Copy(conn, stream); wg.Done() }()
	go func() { io.Copy(stream, conn); wg.Done() }()
	wg.Wait()
}
