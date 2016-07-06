package shared

/*
	DS = common data structures and functions file to be shared by both
	cnc and bot fies.
*/

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"
)

// GO VECTOR Msg
//Msg is the message sent over the network
//Msg is capitolized so GoVecs encoder can acess it
//Furthermore its variables are capitolized to make them public
type Msg struct {
	Content, RealTimestamp string
}

//Node properties
type Node struct {
	NodeID        string // The node id of the node
	IPport        string // ip port where node is listening for other nodes
	UsedSpace     uint64 // how much space this node has used on other nodes
	FreeSpace     uint64 // how much free space this node has available
	LastHeartbeat int    //Relative to the cncs NodeClock, when was the last Heartbeat
}

//Struct a node uses to ask the CNC for nodes it can replicate on
type ReplicationRequest struct {
	FileSize     uint64 //The size of the file the node wants to replicate
	Replications uint64 //The number of replications the node wants
	BotIP        string
	BotID        string
	FilterList   []string // List of IPs to avoid replicating to (because we already have a copy of the file at that location
	GoVec        []byte
}
type Transaction struct {
	ReceiverIP    string // The node that will be accepting this transaction
	SendingIP     string // Node that initiated this transaction
	TransactionID uint64 //The transaction ID to use with file transfers
	Timestamp     time.Time
	FileSize      uint64
	FileName      string
	GoVec         []byte
}

//Struct used by the CNC to reply to a nodes 'replication' request
type ReplicationInfo struct {
	Transactions []Transaction //An array of addresses which the node can replicate on
	Status       Status
}

//Struct used for heartbeats
type Heartbeat struct {
	NodeInfo  *Node
	CNCReply  string //The reply from the CNC
	FirstBeat bool   //If this is the first beat from the node (or perhaps the first beat since a restart)
}

//Struct used for heartbeats
type DeadNodeNotification struct {
	NodeIP string //The ipport of the dead node
	ZorY   string //Either "Z" or "Y" to signifyt which milestone the node has passed
}

// Used to communicate the status of rpc invocations
type Status struct {
	Success int
	Message string
	GoVec   []byte
}

////////////////////////////////////////////////////////////////////////////////
// check error messages
////////////////////////////////////////////////////////////////////////////////
func CheckError(err error) {
	if err != nil {
		log.Fatal("Error:", err)
	}
}

func CheckErrorNonFatal(message string, err error) {
	if err != nil {
		log.Println(message, err)
	}

}

func EncodeTxn(txn *Transaction) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(txn)
	return buf.Bytes(), err
}
func DecodeTxn(b []byte) (Transaction, error) {
	var t Transaction
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&t)
	if err != nil {
		return t, err
	}
	return t, nil

}
