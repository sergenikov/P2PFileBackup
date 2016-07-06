package bot

import (
	"fmt"
	"github.com/arcaneiceman/GoVector/govec"
	"time"
	// "net"
	"bytes"
	"crypto/sha256"
	//	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"github.com/boltdb/bolt"
	"os"
	"project_h2b9_s8z8_y4n8/shared"
	"sync"
)

//Used to transfer files between bots
type FileTransaction struct {
	File          []byte
	SenderAddress string
	NodeID        string
	FileName      string
	Txn           shared.Transaction
	GoVec         []byte
}

/* CONSTANTS */
const HEARTBEAT_FACTOR int = 2

const BOTKEY string = "qwertyuiopasdfghjklzxcvbnmasdfgh" // has to be 32 bytes

/* GLOBALS */
var botInfo shared.Node
var cncrpcip string // rpc address of cnc
var Logger *govec.GoLog
var RecvLogFilePath string = "bot-receive.log"

/* peerFiles are files from other bots. The string is the clients address,
and the array of FileTransaction are each of that clients stored files */
var peerFiles map[string][]FileRequest
var peerFilesMutex *sync.RWMutex

/* Cloud files are the files we have stored on other bots. This is just a map
of file names to bot addresses */
var cloudFiles map[string][]shared.Transaction
var cloudFilesMutex *sync.RWMutex

var db *bolt.DB
var dbMutex *sync.RWMutex

var sendLogFile *os.File
var sendLogMutex *sync.RWMutex

type CNCBotCommunication int  //The RPC interface for communication with the CNC
type Bot2BotCommunication int //The RPC interface for communication with other bots

////////////////////////////////////////////////////////////////////////////////
// Main function that runs bot. Called only from project.go.
////////////////////////////////////////////////////////////////////////////////
func RunBot(botid, cncrpc, localAddress string) {
	//Initialize directories for file storage
	if _, err := os.Stat("retrievedfiles"); os.IsNotExist(err) {
		os.Mkdir("retrievedfiles", os.ModeDir|os.ModePerm)
	}
	if _, err := os.Stat("storedfiles"); os.IsNotExist(err) {
		os.Mkdir("storedfiles", os.ModeDir|os.ModePerm)
	}
	var err error
	db, err = bolt.Open("bot.db", 0600, nil)
	if err != nil {
		shared.CheckError(err)
	}
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("CloudStoredFiles"))
		tx.CreateBucketIfNotExists([]byte("PeerStoredFiles"))
		tx.CreateBucketIfNotExists([]byte("UnverifiedSentTxns"))
		tx.CreateBucketIfNotExists([]byte("UnverifiedReceivedTxns"))
		tx.CreateBucketIfNotExists([]byte("UnverifiedSentDeletions"))
		tx.CreateBucketIfNotExists([]byte("UnverifiedReceivedDeletions"))
		return nil
	})

	dbMutex = &sync.RWMutex{}

	//Initialize a few globals
	botInfo = shared.Node{NodeID: botid, IPport: localAddress, UsedSpace: 0, FreeSpace: 0} //Store the bots details
	peerFiles = make(map[string][]FileRequest)
	peerFilesMutex = &sync.RWMutex{}                   //The mutex for peer files
	cloudFiles = make(map[string][]shared.Transaction) //The map for cloud files
	cloudFilesMutex = &sync.RWMutex{}                  //The mutex for cloud files
	sendLogMutex = &sync.RWMutex{}                     //The mutex for cloud files
	cncrpcip = cncrpc

	Logger = govec.Initialize(botInfo.NodeID+"|"+botInfo.IPport, "clientlogfile") // GO VECTOR Logger
	//checkSendLogs()
	//checkRecvLogs()
	go func() {
		for {
			indexer() // indexer must run every time otherwise nothing in log parsing works
			checkUnverifiedSentDeletions()
			checkUnverifiedReceivedDeletions()
			checkUnverifiedSent()
			checkUnverifiedReceived()
			time.Sleep(2 * time.Second) //Wait for another 10 seconds before the next pass

		}
	}()
	// localAddr, err := net.ResolveUDPAddr("udp", localAddress)
	// shared.CheckError(err)
	// cncAddr, err := net.ResolveUDPAddr("udp", cnchb)
	// shared.CheckError(err)

	hb := make(chan bool)

	go heartbeat()
	go ReadFromStdIn()

	//Open a RPC connection that other nodes can connect to
	go openRPCForBots()
	<-hb
}

////////////////////////////////////////////////////////////////////////////////
// Replicates a file on numReplications nodes
////////////////////////////////////////////////////////////////////////////////
func replicateFile(file []byte, numReplications uint64, fn string) {
	cipher := encryptFile(file)
	fs := uint64(len(cipher))
	hash := sha256.Sum256(file)
	hashString := hex.EncodeToString(hash[:])
	newfn := hashString + "|" + botInfo.NodeID + "|" + fn
	replicationRequestArg := shared.ReplicationRequest{FileSize: fs, Replications: numReplications, BotIP: botInfo.IPport, BotID: botInfo.NodeID}
	replicationRequestArg.FilterList = createFilterList(newfn)
	repinfo, err := requestReplication(&replicationRequestArg)
	if err != nil {
		shared.CheckErrorNonFatal("", err)
		return
	}
	var msg shared.Msg
	Logger.UnpackReceive("Received Transactions", repinfo.Status.GoVec, &msg)

	if err != nil {
		shared.CheckErrorNonFatal("RequestReplicationFailed: ", err)
		return
	}
	for _, txn := range repinfo.Transactions {
		txn.FileName = newfn
		replicateOnBot(cipher, txn, newfn)
	}
}

func encodeFrs(frs []FileRequest) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(frs)
	return buf.Bytes(), err
}
func decodeFrs(b []byte) ([]FileRequest, error) {
	var frs []FileRequest
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&frs)
	if err != nil {
		return frs, err
	}
	return frs, nil

}

func encodeTxns(txns []shared.Transaction) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(txns)
	return buf.Bytes(), err
}
func decodeTxns(b []byte) ([]shared.Transaction, error) {
	var t []shared.Transaction
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&t)
	if err != nil {
		return t, err
	}
	return t, nil

}
func printErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func createFilterList(fn string) []string {
	filterList := make([]string, 0)
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("CloudStoredFiles"))
		btxns := b.Get([]byte(fn))
		if btxns == nil {
			return nil
		}
		txns, err := decodeTxns(btxns)
		if err != nil {
			return err
		}
		for _, txn := range txns {
			filterList = append(filterList, txn.ReceiverIP)
		}

		return nil
	})
	dbMutex.RUnlock()

	return filterList
}
