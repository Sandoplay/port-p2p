package main

import (
	"context"
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

// Стандартні порти для Moonlight/Sunshine
const DefaultPorts = "tcp:47984,tcp:47989,tcp:47990,tcp:48010,udp:47998,udp:47999,udp:48000"

func main() {
	isHost := flag.Bool("host", false, "Працювати як сервер (Sunshine/Game PC)")
	isClient := flag.Bool("client", false, "Працювати як клієнт (Moonlight PC)")
	rendezvous := flag.String("secret", "", "Унікальний секретний пароль (однаковий у обох)")
	portsFlag := flag.String("ports", DefaultPorts, "Список портів (напр. tcp:80,udp:123)")
	flag.Parse()

	if *rendezvous == "" {
		log.Fatal("Будь ласка, вкажіть -secret")
	}
	if (!*isHost && !*isClient) || (*isHost && *isClient) {
		log.Fatal("Потрібно вказати рівно один прапор: -host або -client")
	}

	ctx := context.Background()

	// Ініціалізація libp2p вузла
	node, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		log.Fatalf("Помилка створення вузла: %s", err)
	}
	defer node.Close()

	log.Printf("Ваш Peer ID: %s", node.ID())

	// Налаштування DHT для пошуку один одного
	kDHT, err := dht.New(ctx, node, dht.Mode(dht.ModeAuto))
	if err != nil {
		log.Fatal(err)
	}
	if err = kDHT.Bootstrap(ctx); err != nil {
		log.Fatal(err)
	}

	// Підключаємось до стандартних бутстрап-вузлів
	bootstrap(ctx, node)

	rDiscovery := routingDiscovery.NewRoutingDiscovery(kDHT)
	portList := strings.Split(*portsFlag, ",")

	if *isHost {
		setupHost(ctx, node, rDiscovery, *rendezvous, portList)
	} else {
		setupClient(ctx, node, rDiscovery, *rendezvous, portList)
	}
}

func setupHost(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string) {
	// Обробник для сигналу привітання (щоб не логувати зайвих пірів)
	node.SetStreamHandler(protocol.ID("/p2p-tunnel/hello"), func(s network.Stream) {
		log.Printf(">>> ВАШ ДРУГ ПІДКЛЮЧИВСЯ! (Peer ID: %s)", s.Conn().RemotePeer())
		s.Close()
	})

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))

		node.SetStreamHandler(protoID, func(s network.Stream) {
			targetAddr := fmt.Sprintf("127.0.0.1:%s", port)
			localConn, err := net.Dial(networkType, targetAddr)
			if err != nil {
				log.Printf("Не вдалося підключитися до локального порту %s (%s): %s", port, networkType, err)
				s.Reset()
				return
			}
			log.Printf("Нове з'єднання: %s -> %s", s.Conn().RemotePeer(), targetAddr)
			go syncStreams(localConn, s)
		})
	}

	discoveryUtil.Advertise(ctx, rd, secret)
	fmt.Printf("\n=== СЕРВЕР ЗАПУЩЕНО ===\nСекрет: %s\nПрокидаємо порти: %v\nЧекаємо на клієнта...\n", secret, ports)
	select {}
}

func setupClient(ctx context.Context, node host.Host, rd *routingDiscovery.RoutingDiscovery, secret string, ports []string) {
	fmt.Printf("Шукаємо друга із секретом '%s'...\n", secret)
	var targetPeer peer.ID

	// Цикл пошуку піра
	for targetPeer == "" {
		peerChan, err := rd.FindPeers(ctx, secret)
		if err != nil {
			log.Fatal(err)
		}
		for p := range peerChan {
			if p.ID == node.ID() || len(p.Addrs) == 0 {
				continue
			}
			if err := node.Connect(ctx, p); err == nil {
				targetPeer = p.ID
				break
			}
		}
		if targetPeer == "" {
			time.Sleep(2 * time.Second)
		}
	}

	fmt.Printf("Підключено до друга! (ID: %s)\n", targetPeer)

	// Надсилаємо сигнал "hello" серверу, щоб він знав, що ми тут
	s, err := node.NewStream(ctx, targetPeer, protocol.ID("/p2p-tunnel/hello"))
	if err == nil {
		s.Close()
	}

	for _, pInfo := range ports {
		networkType, port, _ := parsePortInfo(pInfo)
		protoID := protocol.ID(fmt.Sprintf("/p2p-tunnel/%s/%s", networkType, port))
		go startLocalListener(ctx, node, targetPeer, networkType, port, protoID)
	}

	fmt.Printf("\n=== ТУНЕЛЬ ГОТОВИЙ ===\nТепер ви можете підключатися до 127.0.0.1 у Moonlight\n")
	select {}
}

func startLocalListener(ctx context.Context, node host.Host, target peer.ID, networkType, port string, protoID protocol.ID) {
	if networkType == "tcp" {
		ln, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			log.Printf("Помилка прослуховування TCP %s: %s", port, err)
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
		// Для UDP ми створюємо один "слухач"
		ln, err := net.ListenPacket("udp", "127.0.0.1:"+port)
		if err != nil {
			log.Printf("Помилка прослуховування UDP %s: %s", port, err)
			return
		}
		
		// Карта для відстеження активних сесій UDP -> Libp2p Stream
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
				// Запускаємо зворотне читання зі стріму в UDP
				go func(remoteAddr net.Addr, stream network.Stream) {
					respBuf := make([]byte, 65535)
					for {
						rn, err := stream.Read(respBuf)
						if err != nil {
							mu.Lock()
							delete(sessions, remoteAddr.String())
							mu.Unlock()
							return
						}
						ln.WriteTo(respBuf[:rn], remoteAddr)
					}
				}(addr, s)
			}
			mu.Unlock()
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
