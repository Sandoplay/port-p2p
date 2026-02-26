package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
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

const DefaultPorts = "tcp:47984,tcp:47989,tcp:47990,tcp:48010,udp:47998,udp:47999,udp:48000"

type mdnsNotifee struct {
	peerChan chan peer.AddrInfo
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	m.peerChan <- pi
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
	isHost := flag.Bool("host", false, "Працювати як сервер (Sunshine)")
	isClient := flag.Bool("client", false, "Працювати як клієнт (Moonlight)")
	rendezvous := flag.String("secret", "", "Унікальний секретний ідентифікатор")
	portsFlag := flag.String("ports", DefaultPorts, "Список портів")
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
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
		),
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

	bootstrap(ctx, node)

	rDiscovery := routingDiscovery.NewRoutingDiscovery(kDHT)
	portList := strings.Split(*portsFlag, ",")

	if *isHost {
		setupHost(ctx, node, rDiscovery, *rendezvous, portList, mdnsChan)
	} else {
		setupClient(ctx, node, rDiscovery, *rendezvous, portList, mdnsChan)
	}
}

func establishSymmetricConnection(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, mdnsPeers <-chan peer.AddrInfo) peer.ID {
	log.Printf("Початок публікації та пошуку за ідентифікатором: %s", secret)
	discoveryUtil.Advertise(ctx, rd, secret)

	var targetPeer peer.ID
	successChan := make(chan peer.ID, 1)
	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	dialPeer := func(p peer.AddrInfo) {
		if p.ID == node.ID() || len(p.Addrs) == 0 {
			return
		}

		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			log.Printf("Спроба підключення до %s...", pi.ID)
			ctxConn, cancelConn := context.WithTimeout(dialCtx, 7*time.Second)
			defer cancelConn()

			err := node.Connect(ctxConn, pi)
			if err == nil {
				select {
				case successChan <- pi.ID:
				default:
				}
			} else {
				log.Printf("Відмова підключення до %s: %v", pi.ID, err)
			}
		}(p)
	}

	go func() {
		attempt := 1
		for {
			select {
			case <-dialCtx.Done():
				return
			default:
			}

			log.Printf("[Спроба %d] Запит глобального пошуку вузлів...", attempt)
			peerChan, err := rd.FindPeers(dialCtx, secret)
			if err == nil {
				for p := range peerChan {
					dialPeer(p)
				}
			}
			time.Sleep(2 * time.Second)
			attempt++
		}
	}()

	go func() {
		for p := range mdnsPeers {
			select {
			case <-dialCtx.Done():
				return
			default:
				log.Printf("Знайдено локальний вузол: %s", p.ID)
				dialPeer(p)
			}
		}
	}()

	select {
	case targetPeer = <-successChan:
		log.Printf("УСПІХ: Зв'язок на транспортному рівні з %s встановлено", targetPeer)
		cancel()
	case <-ctx.Done():
		log.Printf("Процес ініціалізації перервано")
	}

	return targetPeer
}

func setupHost(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string, mdnsPeers <-chan peer.AddrInfo) {
	node.SetStreamHandler(protocol.ID("/p2p-tunnel/hello"), func(s network.Stream) {
		fmt.Printf("\n>>> З'ЄДНАННЯ ПІДТВЕРДЖЕНО (Peer ID: %s)\n", s.Conn().RemotePeer())
		s.Close()
	})

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))

		node.SetStreamHandler(protoID, func(s network.Stream) {
			targetAddr := fmt.Sprintf("127.0.0.1:%s", port)
			localConn, err := net.Dial(networkType, targetAddr)
			if err != nil {
				log.Printf("Відмова локального підключення до %s: %s", port, err)
				s.Reset()
				return
			}
			if networkType == "udp" {
				go syncFramedStreams(localConn, s)
			} else {
				go syncStreams(localConn, s)
			}
		})
	}

	fmt.Printf("\n=== СЕРВЕР ЗАПУЩЕНО ===\nКонфігурація портів: %v\n", ports)

	targetPeer := establishSymmetricConnection(ctx, node, rd, secret, mdnsPeers)
	log.Printf("ТУНЕЛЬ АКТИВНО (Peer ID: %s)", targetPeer)

	select {}
}

func setupClient(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string, mdnsPeers <-chan peer.AddrInfo) {
	fmt.Printf("\n=== КЛІЄНТ ЗАПУЩЕНО ===\n")
	targetPeer := establishSymmetricConnection(ctx, node, rd, secret, mdnsPeers)
	fmt.Printf("ТУНЕЛЬ АКТИВНО (Peer ID: %s)\n", targetPeer)

	s, err := node.NewStream(ctx, targetPeer, protocol.ID("/p2p-tunnel/hello"))
	if err == nil {
		s.Close()
	}

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))
		go startLocalListener(ctx, node, targetPeer, networkType, port, protoID)
	}

	fmt.Printf("\n=== МАРШРУТИЗАЦІЯ ГОТОВА ===\nІнтерфейс доступу: 127.0.0.1\n")
	select {}
}

func startLocalListener(ctx context.Context, node host.Host, target peer.ID, networkType, port string, protoID protocol.ID) {
	if networkType == "tcp" {
		ln, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			log.Printf("Помилка ініціалізації TCP %s: %s", port, err)
			return
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go func() {
				s, err := node.NewStream(ctx, target, protoID)
				if err != nil {
					conn.Close()
					return
				}
				syncStreams(conn, s)
			}()
		}
	} else if networkType == "udp" {
		ln, err := net.ListenPacket("udp", "127.0.0.1:"+port)
		if err != nil {
			log.Printf("Помилка ініціалізації UDP %s: %s", port, err)
			return
		}

		sessions := make(map[string]network.Stream)
		var mu sync.Mutex
		buf := make([]byte, 65535)

		for {
			n, addr, err := ln.ReadFrom(buf)
			if err != nil {
				continue
			}

			addrStr := addr.String()
			mu.Lock()
			s, ok := sessions[addrStr]
			if !ok {
				s, err = node.NewStream(ctx, target, protoID)
				if err != nil {
					mu.Unlock()
					continue
				}
				sessions[addrStr] = s

				go func(remoteAddr net.Addr, stream network.Stream) {
					lenBuf := make([]byte, 2)
					for {
						if _, err := io.ReadFull(stream, lenBuf); err != nil {
							mu.Lock()
							delete(sessions, remoteAddr.String())
							mu.Unlock()
							return
						}
						length := binary.BigEndian.Uint16(lenBuf)
						dataBuf := make([]byte, length)
						if _, err := io.ReadFull(stream, dataBuf); err != nil {
							mu.Lock()
							delete(sessions, remoteAddr.String())
							mu.Unlock()
							return
						}
						ln.WriteTo(dataBuf, remoteAddr)
					}
				}(addr, s)
			}
			mu.Unlock()
			lenBuf := make([]byte, 2)
			binary.BigEndian.PutUint16(lenBuf, uint16(n))
			s.Write(lenBuf)
			s.Write(buf[:n])
		}
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

func parsePortInfo(p string) (string, string, error) {
	parts := strings.Split(p, ":")
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "tcp", p, nil
}

func bootstrap(ctx context.Context, node host.Host) {
	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		node.Connect(ctx, *pi)
	}
}

func syncFramedStreams(conn net.Conn, stream network.Stream) {
	defer conn.Close()
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 65535)
		lenBuf := make([]byte, 2)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			binary.BigEndian.PutUint16(lenBuf, uint16(n))
			stream.Write(lenBuf)
			stream.Write(buf[:n])
		}
	}()

	go func() {
		defer wg.Done()
		lenBuf := make([]byte, 2)
		for {
			if _, err := io.ReadFull(stream, lenBuf); err != nil {
				return
			}
			length := binary.BigEndian.Uint16(lenBuf)
			dataBuf := make([]byte, length)
			if _, err := io.ReadFull(stream, dataBuf); err != nil {
				return
			}
			conn.Write(dataBuf)
		}
	}()

	wg.Wait()
}
