package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/crypto"
	"github.com/yggdrasil-network/yggdrasil-go/src/yggdrasil"
)

type node struct {
	core              yggdrasil.Core
	config            *config.NodeConfig
	log               *log.Logger
	dhtWaitGroup      sync.WaitGroup
	dhtVisited        map[crypto.BoxPubKey]attempt
	dhtMutex          sync.RWMutex
	nodeInfoWaitGroup sync.WaitGroup
	nodeInfoVisited   struct {
		Meta struct {
			GeneratedAt     int
			NodesSuccessful uint8
			NodesFailed     uint8
		} `json:"meta"`
		NodeInfo map[string]interface{} `json:"nodeinfo"`
	}
	nodeInfoMutex sync.RWMutex
}

type attempt struct {
	coords []uint64 // the coordinates of the node
	found  bool     // has a search for this node completed successfully?
}

func main() {
	n := node{
		config:     config.GenerateConfig(),
		log:        log.New(os.Stdout, "", log.Flags()),
		dhtVisited: make(map[crypto.BoxPubKey]attempt),
		nodeInfoVisited: nodeinfos{
			NodeInfo: make(map[string]interface{}),
		},
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
		go n.dhtPing(pubkey, n.core.Coords())
	} else {
		panic("failed to decode pub key")
	}

	time.Sleep(time.Second)

	n.dhtWaitGroup.Wait()
	n.nodeInfoWaitGroup.Wait()

	n.dhtMutex.Lock()
	n.nodeInfoMutex.Lock()

	fmt.Println()
	fmt.Println("The crawl took", time.Since(starttime))

	attempted := len(n.dhtVisited)
	found := 0
	for _, attempt := range n.dhtVisited {
		if attempt.found {
			found++
		}
	}

	n.nodeInfoVisited.Meta.GeneratedAt = time.Now()
	n.nodeInfoVisited.Meta.NodesSuccessful = len(n.nodeInfoVisited.NodeInfo)
	n.nodeInfoVisited.Meta.NodesFailed = found - len(n.nodeInfoVisited.NodeInfo)

	fmt.Println()
	fmt.Println(attempted, "nodes were processed")
	fmt.Println(found, "nodes were found")
	fmt.Println(attempted-found, "nodes were not found")
	fmt.Println()
	fmt.Println(n.nodeInfoVisited.Meta.NodesSuccessful, "nodes responded with nodeinfo")
	fmt.Println(n.nodeInfoVisited.Meta.NodesFailed, "nodes did not respond with nodeinfo")
	fmt.Println()

	if j, err := json.Marshal(n.nodeInfoVisited); err == nil {
		if err := ioutil.WriteFile("nodeinfo.json", j, 0644); err != nil {
			fmt.Println("Failed to write nodeinfo.json:", err)
		}
	}
}

func (n *node) dhtPing(pubkey crypto.BoxPubKey, coords []uint64) {
	n.dhtWaitGroup.Add(1)
	defer n.dhtWaitGroup.Done()

	n.dhtMutex.RLock()
	if info, ok := n.dhtVisited[pubkey]; (ok && reflect.DeepEqual(coords, info.coords)) || info.found {
		n.dhtMutex.RUnlock()
		return
	}
	n.dhtMutex.RUnlock()

	n.dhtMutex.Lock()
	n.dhtVisited[pubkey] = attempt{
		coords: coords,
		found:  false,
	}
	n.dhtMutex.Unlock()

	time.Sleep(time.Millisecond * time.Duration(rand.Intn(10000)))
	res, err := n.core.DHTPing(
		pubkey,
		coords,
		&crypto.NodeID{},
	)

	n.dhtMutex.Lock()
	n.dhtVisited[pubkey] = attempt{
		coords: res.Coords,
		found:  err == nil,
	}
	if n.dhtVisited[pubkey].found {
		go n.nodeInfo(pubkey, coords)
	} else {
		n.dhtMutex.Unlock()
		return
	}

	n.dhtMutex.Unlock()
	n.dhtMutex.RLock()
	defer time.Sleep(time.Second)
	defer n.dhtMutex.RUnlock()

	for _, info := range res.Infos {
		go n.dhtPing(info.PublicKey, info.Coords)
	}
}

func (n *node) nodeInfo(pubkey crypto.BoxPubKey, coords []uint64) {
	n.nodeInfoWaitGroup.Add(1)
	defer n.nodeInfoWaitGroup.Done()

	nodeid := hex.EncodeToString(pubkey[:])

	n.nodeInfoMutex.RLock()
	if _, ok := n.nodeInfoVisited.NodeInfo[nodeid]; ok {
		n.nodeInfoMutex.RUnlock()
		return
	}
	n.nodeInfoMutex.RUnlock()

	time.Sleep(time.Millisecond * time.Duration(rand.Intn(10000)))
	res, err := n.core.GetNodeInfo(pubkey, coords, false)
	if err != nil {
		return
	}

	n.nodeInfoMutex.Lock()
	defer n.nodeInfoMutex.Unlock()

	var j interface{}
	if err := json.Unmarshal(res, &j); err != nil {
		fmt.Println(err)
	} else {
		n.nodeInfoVisited.NodeInfo[nodeid] = j
	}
}
