package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	routingDiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discoveryUtil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const DefaultPorts = "tcp:47984,tcp:47989,tcp:47990,tcp:48010,udp:47998,udp:47999,udp:48000"

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

	ctx := context.Background()

	node, err := libp2p.New(
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
		setupHost(ctx, node, rDiscovery, *rendezvous, portList)
	} else {
		setupClient(ctx, node, rDiscovery, *rendezvous, portList)
	}
}

func establishSymmetricConnection(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string) peer.ID {
	log.Printf("Початок публікації та пошуку за ідентифікатором: %s", secret)
	discoveryUtil.Advertise(ctx, rd, secret)

	var targetPeer peer.ID
	attempt := 1

	for targetPeer == "" {
		log.Printf("[Спроба %d] Запит FindPeers...", attempt)
		peerChan, err := rd.FindPeers(ctx, secret)
		if err != nil {
			log.Printf("[Спроба %d] Помилка FindPeers: %v", attempt, err)
			time.Sleep(2 * time.Second)
			attempt++
			continue
		}

		peersFound := 0
		for p := range peerChan {
			if p.ID == node.ID() {
				continue
			}
			peersFound++
			log.Printf("Знайдено вузол: %s. Доступні адреси: %v", p.ID, p.Addrs)

			if len(p.Addrs) == 0 {
				log.Printf("Вузол %s ігнорується (відсутні адреси для підключення)", p.ID)
				continue
			}

			log.Printf("Спроба підключення до %s (таймаут 7 сек)...", p.ID)
			ctxConn, cancel := context.WithTimeout(ctx, 7*time.Second)
			err := node.Connect(ctxConn, p)
			cancel()

			if err == nil {
				log.Printf("УСПІХ: Зв'язок на транспортному рівні з %s встановлено", p.ID)
				targetPeer = p.ID
				break
			} else {
				log.Printf("ПОМИЛКА підключення до %s: %v", p.ID, err)
			}
		}

		if targetPeer == "" {
			log.Printf("[Спроба %d] Завершено ітерацію. Оброблено знайдених вузлів: %d. Пауза перед повтором...", attempt, peersFound)
			time.Sleep(2 * time.Second)
			attempt++
		}
	}
	return targetPeer
}

func setupHost(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string) {
	node.SetStreamHandler(protocol.ID("/p2p-tunnel/hello"), func(s network.Stream) {
		fmt.Printf("\n>>> ВАШ ДРУГ ПІДКЛЮЧИВСЯ! (Peer ID: %s)\n", s.Conn().RemotePeer())
		s.Close()
	})

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))

		node.SetStreamHandler(protoID, func(s network.Stream) {
			targetAddr := fmt.Sprintf("127.0.0.1:%s", port)
			localConn, err := net.Dial(networkType, targetAddr)
			if err != nil {
				log.Printf("Відмова підключення до %s: %s", port, err)
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

	targetPeer := establishSymmetricConnection(ctx, node, rd, secret)
	log.Printf("З'ЄДНАННЯ ВСТАНОВЛЕНО (Peer ID: %s)", targetPeer)

	select {}
}

func setupClient(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string) {
	fmt.Printf("\n=== КЛІЄНТ ЗАПУЩЕНО ===\n")
	targetPeer := establishSymmetricConnection(ctx, node, rd, secret)
	fmt.Printf("З'ЄДНАННЯ ВСТАНОВЛЕНО (Peer ID: %s)\n", targetPeer)

	// Відправка сигналу серверу
	s, err := node.NewStream(ctx, targetPeer, protocol.ID("/p2p-tunnel/hello"))
	if err == nil {
		s.Close()
	}

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))
		go startLocalListener(ctx, node, targetPeer, networkType, port, protoID)
	}

	fmt.Printf("\n=== ТУНЕЛЬ ГОТОВИЙ ===\nДоступно для підключення за адресою 127.0.0.1\n")
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
