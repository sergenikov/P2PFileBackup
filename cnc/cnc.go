package cnc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/arcaneiceman/GoVector/govec"
	"github.com/boltdb/bolt"
	"log"
	"net"
	"net/rpc"
	"project_h2b9_s8z8_y4n8/shared"
	"strconv"
	"sync"
	"time"
)

type CNCInformation struct {
	ipport string
}

// initial storage for every node
const INITIALSTORAGE uint64 = 15000

// time limit until a requested transaction timesout
const TRANSACTIONTIMEOUT uint64 = 5

/* GLOBALS */
var CNCInfo CNCInformation
var Logger *govec.GoLog
var NODECLOCKINTERVAL int    //(seconds) How often should the NodeClock be updated
var NodeClock int            //This is a simple integer used to watch for dead nodes.
var DEADNODESCANINTERVAL int //(seconds) How often should we scan for dead nodes
var TIMEY int                //How many NodeClock ticks until a node is in time Y
var TIMEZ int                //How many NodeClock ticks until a node is in time Z

type CNCBotCommunication int //The RPC interface for communication with the CNC

// IPPORT:*Node
var liveNodes map[string]*shared.Node
var liveNodesMutex *sync.RWMutex
var nodesInZ map[string]*shared.Node
var nodesInZMutex *sync.RWMutex
var nodesInY map[string]*shared.Node
var nodesInYMutex *sync.RWMutex

var maxreplications uint64
var maxfilesize uint64

var blacklist map[string]*shared.Node
var transactionID uint64 = 0

var db *bolt.DB
var dbMutex *sync.RWMutex

////////////////////////////////////////////////////////////////////////////////
// check error messages
////////////////////////////////////////////////////////////////////////////////
// func checkError(err error) {
// 	if err != nil {
// 		log.Fatal("Error:", err)
// 	}
// }

////////////////////////////////////////////////////////////////////////////////
// Just prints what nodes are in liveNodes map in a separate thread
// This should be useful (and will be expanded) when we are doing write queries
// and checking what nodes have enough space for a write
// Locks used:
// 	liveNodes
////////////////////////////////////////////////////////////////////////////////
func printLiveNodes() {
	for {
		time.Sleep(2 * time.Second)
		fmt.Printf("Live nodes:\n")
		liveNodesMutex.RLock()
		for _, node := range liveNodes {
			fmt.Printf("	%s - FreeSpace: %v / UsedSpace: %v\n", node.NodeID, node.FreeSpace, node.UsedSpace)
		}
		liveNodesMutex.RUnlock()
	}
}

////////////////////////////////////////////////////////////////////////////////
// Starts cnc server. Called from project.go only
////////////////////////////////////////////////////////////////////////////////
func RunCnc(cncrpcip string, rfactor, mfs uint64) {

	/* LISTEN FOR NEW CLIENTS  - separate thread */
	// thread per client? - not needed for udp
	/*
		ultimately needs two ports:
		- list for heartbeats from existing nodes UDP
		- accept new connections from nodes UDP
	*/

	///////////////////////////////////
	// Init globals, mutexes and channels first
	///////////////////////////////////
	var err error
	db, err = bolt.Open("cnc.db", 0600, nil)
	if err != nil {
		shared.CheckError(err)
	}
	dberr := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("Storage"))
		tx.CreateBucketIfNotExists([]byte("Pending"))
		tx.CreateBucketIfNotExists([]byte("Completed"))
		tx.CreateBucketIfNotExists([]byte("TransactionID"))
		tx.CreateBucketIfNotExists([]byte("PendingDeletion"))
		tx.CreateBucketIfNotExists([]byte("Deleted"))
		b := tx.Bucket([]byte("TransactionID"))
		tid := b.Get([]byte("TransactionID"))
		if tid == nil {
			transactionID = 0

		} else {
			var err error
			tidstr := string(tid)
			transactionID, err = strconv.ParseUint(tidstr, 10, 64)
			return err

		}

		return nil
	})
	if dberr != nil {
		shared.CheckError(err)
	}
	liveNodesMutex = &sync.RWMutex{}
	dbMutex = &sync.RWMutex{}
	nodesInZMutex = &sync.RWMutex{}
	nodesInYMutex = &sync.RWMutex{}
	liveNodes = make(map[string]*shared.Node)
	nodesInZ = make(map[string]*shared.Node)
	nodesInY = make(map[string]*shared.Node)
	listenerChan := make(chan bool)
	maxreplications = rfactor
	maxfilesize = mfs
	Logger = govec.Initialize("cnc", "serverLogFile") // GO VECTOR Logger
	NODECLOCKINTERVAL = 2                             //(seconds) How often should the NodeClock be updated
	NodeClock = 0
	DEADNODESCANINTERVAL = 5 //(seconds) How often should we scan for dead nodes
	TIMEY = 5                //How many NodeClock ticks until a node is in time Y
	TIMEZ = 7                //How many NodeClock ticks until a node is in time Z

	go func() { //Update the NodeClock
		for {
			NodeClock++
			time.Sleep(time.Duration(NODECLOCKINTERVAL) * time.Second) //Wait for another 10 seconds before updating
		}
	}()
	go scanForDeadNodes() //Start looking for dead nodes
	go openRPC(cncrpcip)

	go printLiveNodes()

	<-listenerChan
}

func (cbc *CNCBotCommunication) RequestDeletion(args *shared.Transaction, reply *shared.Status) error {
	dbMutex.Lock()
	var msg shared.Msg
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("PendingDeletion"))
		cb := tx.Bucket([]byte("Completed"))
		db := tx.Bucket([]byte("Deleted"))
		s := strconv.FormatUint(args.TransactionID, 10)
		gobtxn := cb.Get([]byte(s))
		if gobtxn == nil {
			// no need to continue
			return errors.New("No such transaction number")
		}
		deltxn := db.Get([]byte(s))
		if deltxn != nil {
			return errors.New("File has already been verified as being deleted")
		}

		txnenc, err := shared.EncodeTxn(args)
		if err != nil {
			return err
		}
		err = b.Put([]byte(s), txnenc)
		if err != nil {
			return err
		}
		return err
	})
	dbMutex.Unlock()
	if dberr != nil {
		msg = shared.Msg{dberr.Error(), time.Now().String()}
		buf := Logger.PrepareSend(dberr.Error(), msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = dberr.Error()
		return nil
	}
	msg = shared.Msg{"Deletion approved", time.Now().String()}
	buf := Logger.PrepareSend("Deletion approved", msg)
	reply.GoVec = buf
	reply.Success = 1
	return nil

}

////////////////////////////////////////////////////////////////////////////////
// Nodes call this to request a list of nodes which it can replicate a file on
////////////////////////////////////////////////////////////////////////////////
func (cbc *CNCBotCommunication) GetNodes(args *shared.ReplicationRequest, reply *shared.ReplicationInfo) error {
	//Logger.LogLocalEvent("Received GetNodes RPC call: " + fmt.Sprintf("%#v", *args))
	var nodesFound uint64 = 0 //Keep track of how many suitable nodes we currently have for replication
	transactions := make([]shared.Transaction, 0)
	var msg shared.Msg

	Logger.UnpackReceive("Received replication request: ", args.GoVec, &msg)

	liveNodesMutex.RLock()
	fmt.Printf("Replication factor: %+v\n", maxreplications)
	fmt.Printf("LiveNodes: %+v\n", liveNodes)
	if args.FileSize > maxfilesize {
		// need to return failure
		msg = shared.Msg{"Issued transactions", time.Now().String()}
		buf := Logger.PrepareSend("Filesize too large", msg)
		reply.Status.GoVec = buf
		reply.Status.Success = 0
		reply.Status.Message = "Filesize is too large"
		return nil
	}
	dbMutex.RLock()
	dberr := db.View(func(tx *bolt.Tx) error {
		// We need to check that this node has enough storage space
		sb := tx.Bucket([]byte("Storage"))
		v := sb.Get([]byte(args.BotIP))
		n, err := decodeNode(v)
		if err != nil {
			return err
		}
		remainingspace := INITIALSTORAGE - n.UsedSpace
		r := min(args.Replications, maxreplications)
		if remainingspace < r*args.FileSize {
			return errors.New("Not enough storage available for transaction")
		}
		return nil
	})
	dbMutex.RUnlock()
	if dberr != nil {
		msg = shared.Msg{dberr.Error(), time.Now().String()}
		buf := Logger.PrepareSend(dberr.Error(), msg)
		reply.Status.GoVec = buf
		reply.Status.Success = 0
		reply.Status.Message = dberr.Error()
		return nil
	}

	for nodeAddr, node := range liveNodes {
		if nodesFound == maxreplications || nodesFound == args.Replications {
			fmt.Println("good on nodes")
			//Already have enough nodes
			break
		}
		if nodeAddr == args.BotIP {
			continue
		}
		if node.FreeSpace <= args.FileSize {
			// this node doesn't have enough storage space
			continue
		}
		var skipIP bool = false
		for _, filterIP := range args.FilterList {
			if nodeAddr == filterIP {
				skipIP = true
			}
		}
		if skipIP {
			continue

		}
		newtxn := shared.Transaction{nodeAddr, args.BotIP, transactionID, time.Now(), args.FileSize, "", make([]byte, 0)}
		dbMutex.Lock()
		dberr := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("Pending"))
			btid := tx.Bucket([]byte("TransactionID"))
			txnenc, err := shared.EncodeTxn(&newtxn)
			if err != nil {
				return err
			}
			s := strconv.FormatUint(transactionID, 10)
			err = b.Put([]byte(s), txnenc)
			if err != nil {
				return err

			}
			err = btid.Put([]byte("TransactionID"), []byte(s))
			return err
		})
		dbMutex.Unlock()
		if dberr != nil {
			msg = shared.Msg{dberr.Error(), time.Now().String()}
			buf := Logger.PrepareSend(dberr.Error(), msg)
			reply.Status.GoVec = buf
			reply.Status.Success = 0
			reply.Status.Message = dberr.Error()
			return nil
		}

		transactions = append(transactions, newtxn)
		fmt.Printf("New transaction for node: %s with id: %d\n", nodeAddr, transactionID)
		transactionID++
		nodesFound++
	}
	liveNodesMutex.RUnlock()
	msg = shared.Msg{"Issued transactions", time.Now().String()}
	buf := Logger.PrepareSend("Issued "+strconv.FormatUint(nodesFound, 10)+" transactions to "+args.BotID+"|"+args.BotIP, msg)
	reply.Status.GoVec = buf
	reply.Transactions = transactions
	reply.Status.Success = 1
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Nodes call this to issue a heartbeat. Args and reply should point to the same thing
////////////////////////////////////////////////////////////////////////////////
func (cbc *CNCBotCommunication) Heartbeat(args *shared.Heartbeat, reply *shared.Heartbeat) error {
	//Logger.LogLocalEvent("Hearbeat: " + fmt.Sprintf("%#v", args.NodeInfo.NodeID))
	nodeIP := args.NodeInfo.IPport
	var dberr error
	args.NodeInfo.LastHeartbeat = NodeClock //Set the Nodes heartbeat clock so it isn't considered dead
	if args.FirstBeat {                     //If this node is conencting to the CNC for the first time (or after a restart)
		//Add the node to the map
		liveNodesMutex.Lock()
		dbMutex.Lock()
		dberr = db.Update(func(tx *bolt.Tx) error {
			sb := tx.Bucket([]byte("Storage"))
			v := sb.Get([]byte(nodeIP))
			fmt.Printf("NODE IN DB: %s\n", v)
			var err error
			if v == nil {
				var b []byte
				args.NodeInfo.FreeSpace = INITIALSTORAGE
				args.NodeInfo.UsedSpace = 0
				b, err = encodeNode(args.NodeInfo)
				if err != nil {
					return err
				}
				err = sb.Put([]byte(nodeIP), b)

			} else {
				n, err := decodeNode(v)
				if err != nil {
					return err
				}
				args.NodeInfo.UsedSpace = n.UsedSpace
				args.NodeInfo.FreeSpace = n.FreeSpace

			}

			return err
		})
		dbMutex.Unlock()
		liveNodes[nodeIP] = args.NodeInfo
		liveNodesMutex.Unlock()
	}
	if dberr != nil {
		shared.CheckErrorNonFatal("Error in heartbeat", dberr)
	}
	//We still need to update the nodes last heartbeat to prevent it from being killed off
	liveNodesMutex.Lock()
	liveNodes[nodeIP].LastHeartbeat = NodeClock
	liveNodesMutex.Unlock()
	reply.CNCReply = "OK"
	fmt.Printf("Heartbeat from node: " + args.NodeInfo.NodeID + "\n")
	return nil
}

func (cbc *CNCBotCommunication) VerifyTransaction(args *shared.Transaction, rep *shared.Status) error {
	dbMutex.RLock()
	s := strconv.FormatUint(args.TransactionID, 10)
	var msg shared.Msg
	Logger.UnpackReceive("Verifying Completion txid= "+s, args.GoVec, &msg)
	dberr := db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket([]byte("Completed"))
		gobtxn := cb.Get([]byte(s))
		if gobtxn == nil {
			// no need to continue
			return errors.New("No such transaction number")
		}
		return nil
	})
	dbMutex.RUnlock()

	if dberr != nil {
		msg = shared.Msg{dberr.Error() + " txid= " + s, time.Now().String()}
		buf := Logger.PrepareSend(dberr.Error()+" txid= "+s, msg)
		rep.GoVec = buf
		rep.Success = 0
		rep.Message = dberr.Error()
		return nil
	}
	msg = shared.Msg{"Verified Transaction txid= " + s, time.Now().String()}
	buf := Logger.PrepareSend(msg.Content, msg)
	rep.GoVec = buf
	rep.Success = 1
	rep.Message = msg.Content

	return nil
}

func (cbc *CNCBotCommunication) VerifyDeletion(args *shared.Transaction, rep *shared.Status) error {
	dbMutex.RLock()
	s := strconv.FormatUint(args.TransactionID, 10)
	var msg shared.Msg
	Logger.UnpackReceive("Verified Deletion txid= "+s, args.GoVec, &msg)
	dberr := db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket([]byte("Deleted"))
		gobtxn := cb.Get([]byte(s))
		if gobtxn == nil {
			// no need to continue
			return errors.New("No such transaction number")
		}
		return nil
	})
	dbMutex.RUnlock()

	if dberr != nil {
		msg = shared.Msg{dberr.Error() + " txid= " + s, time.Now().String()}
		buf := Logger.PrepareSend(dberr.Error()+" txid= "+s, msg)
		rep.GoVec = buf
		rep.Success = 0
		rep.Message = dberr.Error()
		return nil
	}
	rep.Success = 1
	rep.Message = "Deletion verified"

	return nil
}

func (cbc *CNCBotCommunication) CompleteTransaction(args *shared.Transaction, rep *shared.Status) error {
	dbMutex.Lock()
	s := strconv.FormatUint(args.TransactionID, 10)
	var msg shared.Msg
	Logger.UnpackReceive("Beginning to complete transaction txid= "+s, args.GoVec, &msg)
	dberr := db.Update(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte("Pending"))
		cb := tx.Bucket([]byte("Completed"))
		gobtxn := pb.Get([]byte(s))
		if gobtxn == nil {
			// no need to continue
			return errors.New("No such transaction number")
		}
		// put the transaction into completed
		cb.Put([]byte(s), gobtxn)
		txn, err := shared.DecodeTxn(gobtxn)
		if err != nil {
			return err
		}
		if args.FileSize != txn.FileSize {
			return errors.New("Incorrect filesize")
		}
		curTime := time.Now()
		diff := curTime.Sub(txn.Timestamp)
		if uint64(diff.Minutes()) > TRANSACTIONTIMEOUT {
			err = pb.Delete([]byte(s))
			return errors.New("Transaction has timed out, please request a new transaction")
		}
		err = pb.Delete([]byte(s))
		if err != nil {
			return err
		}
		sb := tx.Bucket([]byte("Storage"))

		// Subtract available space on the node the node that's receiving the file
		recvnodeIP := args.ReceiverIP
		recvnodeBytes := sb.Get([]byte(recvnodeIP))
		var recvn shared.Node
		recvn, err = decodeNode(recvnodeBytes)
		recvn.FreeSpace -= args.FileSize
		nbytes, err := encodeNode(&recvn)
		err = sb.Put([]byte(recvnodeIP), nbytes)
		if err != nil {
			return err
		}

		fmt.Println("Filesize: ", args.FileSize)

		// Increment the amount of storage used by the sender
		sendnodeIP := args.SendingIP
		sendnodeBytes := sb.Get([]byte(sendnodeIP))
		var sendn shared.Node
		sendn, err = decodeNode(sendnodeBytes)
		sendn.UsedSpace += args.FileSize
		sendnbytes, err := encodeNode(&sendn)
		err = sb.Put([]byte(sendnodeIP), sendnbytes)
		return err
	})
	dbMutex.Unlock()

	if dberr != nil {
		msg := shared.Msg{dberr.Error() + " txid= " + s, time.Now().String()}
		buf := Logger.PrepareSend(dberr.Error()+" txid= "+s, msg)
		rep.GoVec = buf
		shared.CheckErrorNonFatal("Completed Transaction: ", dberr)
		rep.Success = 0
		rep.Message = dberr.Error()
		return nil
	}

	msg = shared.Msg{"Completed Transaction txid= " + s, time.Now().String()}
	buf := Logger.PrepareSend("Completed Transaction txid= "+s, msg)
	rep.GoVec = buf
	rep.Success = 1
	rep.Message = "File transaction completed"
	return nil
}

func (cbc *CNCBotCommunication) CompleteDeletion(args *shared.Transaction, rep *shared.Status) error {
	dbMutex.Lock()
	var msg shared.Msg
	Logger.UnpackReceive("Beginning deletion: ", args.GoVec, &msg)
	dberr := db.Update(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte("PendingDeletion"))
		db := tx.Bucket([]byte("Deleted"))
		s := strconv.FormatUint(args.TransactionID, 10)
		gobtxn := pb.Get([]byte(s))
		if gobtxn == nil {
			// no need to continue
			return errors.New("No such transaction number")
		}

		// delete from Pending Deletion and put in Deleted
		err := db.Put([]byte(s), gobtxn)
		if err != nil {
			return err
		}
		err = pb.Delete([]byte(s))
		if err != nil {
			return err
		}

		sb := tx.Bucket([]byte("Storage"))

		// Add available space on the node the node that's receiving the file
		recvnodeIP := args.ReceiverIP
		recvnodeBytes := sb.Get([]byte(recvnodeIP))
		var recvn shared.Node
		recvn, err = decodeNode(recvnodeBytes)
		recvn.FreeSpace += args.FileSize
		nbytes, err := encodeNode(&recvn)
		err = sb.Put([]byte(recvnodeIP), nbytes)
		if err != nil {
			return err
		}

		fmt.Println("Filesize: ", args.FileSize)

		// Decrement the amount of storage used by the sender
		sendnodeIP := args.SendingIP
		sendnodeBytes := sb.Get([]byte(sendnodeIP))
		var sendn shared.Node
		sendn, err = decodeNode(sendnodeBytes)
		sendn.UsedSpace -= args.FileSize
		sendnbytes, err := encodeNode(&sendn)
		err = sb.Put([]byte(sendnodeIP), sendnbytes)
		return err
	})
	dbMutex.Unlock()

	if dberr != nil {
		msg := shared.Msg{"Could not delete transaction", time.Now().String()}
		buf := Logger.PrepareSend("Could not delete transaction", msg)
		rep.GoVec = buf
		shared.CheckErrorNonFatal("Complete deletion: ", dberr)
		rep.Success = 0
		rep.Message = dberr.Error()
		return nil
	}

	msg = shared.Msg{"Completed deletion", time.Now().String()}
	buf := Logger.PrepareSend("Completed deletion", msg)
	rep.GoVec = buf
	rep.Success = 1
	rep.Message = "Deletion completed"
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Opens a RPC connection that nodes can connect to
////////////////////////////////////////////////////////////////////////////////
func openRPC(rpcip string) {
	//Logger.LogLocalEvent("Opening RPC")
	cbc := new(CNCBotCommunication)
	rpc.Register(cbc)
	l, e := net.Listen("tcp", rpcip)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//Start listening for RPC commands in a separate routine
	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}

func encodeNode(n *shared.Node) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(n)
	return buf.Bytes(), err
}

func decodeNode(b []byte) (shared.Node, error) {
	var n shared.Node
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&n)
	if err != nil {
		return n, err
	}
	return n, nil
}

////////////////////////////////////////////////////////////////////////////////
// Scans for dead nodes
////////////////////////////////////////////////////////////////////////////////
func scanForDeadNodes() {
	for {
		//Logger.LogLocalEvent(fmt.Sprintf("Scanning for dead nodes. NodeClock: %d", NodeClock))
		//First scan for dead nodes in the liveNodes map and move them into Y time
		liveNodesMutex.RLock()
		for _, node := range liveNodes {
			if node.LastHeartbeat < NodeClock-TIMEY {
				//Lets call this time Y
				Logger.LogLocalEvent(fmt.Sprintf("\t%v has now past time Y", node.NodeID))
				//Add the node the the nodesInY map
				nodesInYMutex.Lock()
				nodesInY[node.IPport] = node
				nodesInYMutex.Unlock()
				//Remove the node from the liveNodes map
				liveNodesMutex.RUnlock() //First we have to release our read lock to acquire the write lock
				liveNodesMutex.Lock()
				delete(liveNodes, node.IPport)
				liveNodesMutex.Unlock() //Now let go of the write lock to reacquire the read lock
				liveNodesMutex.RLock()
				notifyOfDeadNode(node, "Y")
			}
		}
		liveNodesMutex.RUnlock()

		//Next scan for dead nodes in the nodesInY map and move them into Z time
		nodesInYMutex.RLock()
		for _, node := range nodesInY {
			if node.LastHeartbeat < NodeClock-TIMEZ {
				Logger.LogLocalEvent(fmt.Sprintf("\t%v has now past time Z", node.NodeID))
				//Add the node the the nodesInZ map
				nodesInZMutex.Lock()
				nodesInZ[node.IPport] = node
				nodesInZMutex.Unlock()
				//Remove the node from the liveNodes map
				nodesInYMutex.RUnlock() //First we have to release our read lock to acquire the write lock
				nodesInYMutex.Lock()
				delete(nodesInY, node.IPport)
				nodesInYMutex.Unlock() //Now let go of the write lock to reacquire the read lock
				nodesInYMutex.RLock()
				notifyOfDeadNode(node, "Z")
				dbMutex.Lock()
				db.Update(func(tx *bolt.Tx) error {
					sb := tx.Bucket([]byte("Storage"))
					nodebytes := sb.Get([]byte(node.IPport))
					// Add available space on the node the node that's receiving the file

					// Decrement the amount of storage used by the sender
					var n shared.Node
					n, err := decodeNode(nodebytes)
					if err != nil {
						return err
					}
					n.UsedSpace = 0
					n.FreeSpace = INITIALSTORAGE
					nbytes, err := encodeNode(&n)
					err = sb.Put([]byte(node.IPport), nbytes)
					return err
				})
				dbMutex.Unlock()
			}
		}
		nodesInYMutex.RUnlock()

		//Logger.LogLocalEvent("Finished dead node scan")
		time.Sleep(time.Duration(DEADNODESCANINTERVAL) * time.Second) //Wait for another 10 seconds before the next pass
	}
}

////////////////////////////////////////////////////////////////////////////////
// Notifies nodes that a node has died
// Needs the ipport of the dead node and the milestone it has passed (Z or Y)
////////////////////////////////////////////////////////////////////////////////
func notifyOfDeadNode(node *shared.Node, milestone string) {
	Logger.LogLocalEvent("Sending DeadNodeNotification for node: " + node.NodeID)
	args := shared.DeadNodeNotification{NodeIP: node.IPport, ZorY: milestone}
	liveNodesMutex.RLock()
	for _, node := range liveNodes {
		// Connect to the Bots RPC service.
		cbc, err := rpc.Dial("tcp", node.IPport)
		if err != nil {
			liveNodesMutex.RUnlock()
			return
		}
		var tids []uint64
		err = cbc.Call("CNCBotCommunication.DeadNodeNotification", args, &tids) //Query the CNC
		shared.CheckErrorNonFatal("", err)
		dbMutex.Lock()
		db.Update(func(tx *bolt.Tx) error {
			sb := tx.Bucket([]byte("Storage"))
			nodebytes := sb.Get([]byte(node.IPport))
			var n shared.Node
			n, err := decodeNode(nodebytes)
			if err != nil {
				return err
			}
			cb := tx.Bucket([]byte("Completed"))
			for _, tid := range tids {
				tidstr := strconv.FormatUint(tid, 10)
				enctxn := cb.Get([]byte(tidstr))
				if enctxn == nil {
					continue
				}
				txn, err := shared.DecodeTxn(enctxn)
				if err != nil {
					return err

				}
				if milestone == "Z" {
					n.FreeSpace += txn.FileSize
				} else {
					n.UsedSpace -= txn.FileSize

				}
			}
			// Add available space on the node the node that's receiving the file

			// Decrement the amount of storage used by the sender
			nbytes, err := encodeNode(&n)
			err = sb.Put([]byte(node.IPport), nbytes)
			return err
		})
		dbMutex.Unlock()
	}
	liveNodesMutex.RUnlock()
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b

}
