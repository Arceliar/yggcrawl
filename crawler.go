package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/crypto"
	"github.com/yggdrasil-network/yggdrasil-go/src/yggdrasil"
)

type node struct {
	core   yggdrasil.Core
	config *config.NodeConfig
	log    *log.Logger

	dhtGroup   sync.WaitGroup
	dhtVisited map[crypto.BoxPubKey]attempt
	dhtMutex   sync.RWMutex

	niGroup   sync.WaitGroup
	niVisited map[crypto.BoxPubKey]interface{}
	niMutex   sync.RWMutex
}

type attempt struct {
	attempts uint8    // how many times have we tried to search for this node?
	coords   []uint64 // the coordinates of the node
	found    bool     // has a search for this node completed successfully?
	active   bool     // is a search taking place right now?
}

func main() {
	n := node{
		config:     config.GenerateConfig(),
		log:        log.New(os.Stdout, "", log.Flags()),
		dhtVisited: make(map[crypto.BoxPubKey]attempt),
		niVisited:  make(map[crypto.BoxPubKey]interface{}),
	}

	n.config.NodeInfo = map[string]interface{}{
		"name": "Yggdrasil Crawler",
	}
	n.config.AdminListen = "unix:///var/run/yggcrawl.sock"
	n.config.SessionFirewall.Enable = true
	n.config.SessionFirewall.AllowFromDirect = false
	n.config.SessionFirewall.AllowFromRemote = false
	n.config.SessionFirewall.AlwaysAllowOutbound = false

	n.core.Start(n.config, n.log)
	n.core.AddPeer("tcp://edinburgh.psg.neilalexander.dev:54321", "")

	fmt.Println("Our public key is", n.core.EncryptionPublicKey())

	fmt.Println("Waiting for DHT bootstrap")
	for {
		if len(n.core.GetDHT()) > 3 {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println("DHT bootstrap complete")

	starttime := time.Now()

	var pubkey crypto.BoxPubKey
	if key, err := hex.DecodeString(n.core.EncryptionPublicKey()); err == nil {
		copy(pubkey[:], key)
		go n.dhtPing(pubkey, n.core.Coords(), 0)
	} else {
		panic("failed to decode pub key")
	}

	time.Sleep(time.Second)

	n.dhtGroup.Wait()
	n.niGroup.Wait()

	n.dhtMutex.Lock()
	n.niMutex.Lock()

	fmt.Println()
	fmt.Println("DHT ping workers have finished")
	fmt.Println("The crawl took", time.Since(starttime), "seconds")

	attempted := len(n.dhtVisited)
	found := 0
	for _, attempt := range n.dhtVisited {
		if attempt.found {
			found++
		}
	}

	fmt.Println(attempted, "nodes were processed")
	fmt.Println(found, "nodes were found")
	fmt.Println(attempted-found, "nodes were not found")
	fmt.Println(len(n.niVisited), "nodes responded with nodeinfo")
	fmt.Println(found-len(n.niVisited), "nodes did not respond with nodeinfo")
}

func (n *node) dhtPing(pubkey crypto.BoxPubKey, coords []uint64, delay uint8) {
	n.dhtGroup.Add(1)
	defer n.dhtGroup.Done()
	defer fmt.Printf(".")

	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	n.dhtMutex.RLock()
	if info, ok := n.dhtVisited[pubkey]; ok && info.active {
		n.dhtMutex.RUnlock()
		return
	}
	n.dhtMutex.RUnlock()

	n.dhtMutex.Lock()
	n.dhtVisited[pubkey] = attempt{
		coords: coords,
		active: true,
	}
	n.dhtMutex.Unlock()

	res, err := n.core.DHTPing(
		pubkey,
		coords,
		&crypto.NodeID{},
	)

	n.dhtMutex.Lock()
	attempt := n.dhtVisited[pubkey]
	if err == nil {
		attempt.found = true
		n.dhtVisited[pubkey] = attempt
		n.dhtMutex.Unlock()
		go n.nodeInfo(pubkey, coords)
	} else {
		attempt.attempts++
		n.dhtVisited[pubkey] = attempt
		n.dhtMutex.Unlock()
		return
	}

	n.dhtMutex.RLock()
	defer n.dhtMutex.RUnlock()

	for _, info := range res.Infos {
		attempt, ok := n.dhtVisited[info.PublicKey]
		if !ok || !attempt.found {
			go n.dhtPing(info.PublicKey, info.Coords, 0)
		}
	}

	for pubkey, info := range n.dhtVisited {
		if !info.found && !info.active && info.attempts < 5 {
			go n.dhtPing(pubkey, info.coords, info.attempts)
		}
	}
}

func (n *node) nodeInfo(pubkey crypto.BoxPubKey, coords []uint64) {
	n.niGroup.Add(1)
	defer n.niGroup.Done()
	defer fmt.Printf(":")

	n.niMutex.RLock()
	if _, ok := n.niVisited[pubkey]; ok {
		n.niMutex.RUnlock()
		return
	}
	n.niMutex.RUnlock()

	res, err := n.core.GetNodeInfo(pubkey, coords, false)
	if err != nil {
		return
	}

	n.niMutex.Lock()
	defer n.niMutex.Unlock()

	n.niVisited[pubkey] = res
}
