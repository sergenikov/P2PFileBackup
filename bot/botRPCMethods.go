package bot

import (
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"project_h2b9_s8z8_y4n8/shared"
	"strconv"
	"strings"
	"time"
)

//Used to retrieve a file
type FileRequest struct {
	NodeID        string
	SenderAddress string
	FileName      string
	TransactionID uint64
}

////////////////////////////////////////////////////////////////////////////////
// Opens a RPC connection that other bots can connect to
////////////////////////////////////////////////////////////////////////////////
func openRPCForBots() {
	fmt.Println("RPC opened")
	//Register the Bot2BotCommunication
	b2b := new(Bot2BotCommunication)
	rpc.Register(b2b)
	//Register the CNC2BotCommunication
	c2b := new(CNCBotCommunication)
	rpc.Register(c2b)
	l, e := net.Listen("tcp", botInfo.IPport)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//Start listening for RPC commands in a separate routine
	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Other bots call this method to retrieve a file they have stored here
////////////////////////////////////////////////////////////////////////////////
func (b2b *Bot2BotCommunication) RetrieveFile(args *FileRequest, reply *FileTransaction) error {
	dbMutex.RLock()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("PeerStoredFiles"))
		bfrs := b.Get([]byte(args.SenderAddress))
		existingFiles, _ := decodeFrs(bfrs)
		for _, file := range existingFiles { //Loop over them, looking for the correct file
			if file.FileName == args.FileName { //This is the correct file
				fileloc := filepath.Join("storedfiles", args.NodeID, args.FileName)
				if _, err := os.Stat(fileloc); os.IsNotExist(err) {
					reply.FileName = "null"
					return nil
					// errror
				}
				fbytes, _ := ioutil.ReadFile(fileloc)
				reply.File = fbytes
				reply.FileName = file.FileName
				reply.Txn.TransactionID = file.TransactionID
				reply.SenderAddress = file.SenderAddress
				break
			}
		}
		return nil
	})
	dbMutex.RUnlock()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Other bots call this method to store their file here
// RECEIVER logging order
// - receive file
// - write to disk
// - log receive
// - send ack - ok RPC reply
//////////////////////////////////////////////////////////////////////////////
func (b2b *Bot2BotCommunication) StoreFile(args *FileTransaction, reply *shared.Status) error {
	//fr := FileRequest{botInfo.NodeID, args.SenderAddress, args.FileName, args.Txn.TransactionID}

	var msg shared.Msg
	saveloc := filepath.Join("storedfiles", args.NodeID)
	tidstr := strconv.FormatUint(args.Txn.TransactionID, 10)
	if stat, err := os.Stat(saveloc); os.IsNotExist(err) || !stat.IsDir() {
		os.MkdirAll(saveloc, os.ModeDir|os.ModePerm)
	}
	fp := filepath.Join(saveloc, args.FileName)
	if _, err := os.Stat(fp); err == nil {
		msg.Content = "File already exists"
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend("File already exists", msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = "File already exists"
		return nil
	}

	err := ioutil.WriteFile(fp, args.File, 0666)
	if err != nil {
		msg.Content = err.Error()
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(err.Error(), msg)
		reply.GoVec = buf
		shared.CheckErrorNonFatal("Failed to write file: ", err)
		reply.Success = 0
		reply.Message = err.Error()
		return nil
	}
	Logger.UnpackReceive("File has been saved to disk", args.GoVec, &msg)
	dberr := addToUnverifiedReceivedDB(args.Txn)
	if dberr != nil {
		msg.Content = dberr.Error()
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(dberr.Error(), msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = "Could not store file"
		shared.CheckErrorNonFatal("Dberr: ", dberr)
		return nil

	}
	Logger.LogLocalEvent("Transaction txid= " + tidstr + " is marked as unverified")

	cbc, err := rpc.Dial("tcp", cncrpcip)
	if err != nil {
		// CNC is down or network failure occured,
		removeTransactionFromUnverifiedReceived(args.Txn.TransactionID)
		os.Remove(fp)
		shared.CheckErrorNonFatal("Could not reach CNC", err)
		msg.Content = err.Error() + " txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = msg.Content
		return nil

	}
	msg.Content = "Verifying txn with CNC txid= " + tidstr
	msg.RealTimestamp = time.Now().String()
	buf := Logger.PrepareSend(msg.Content, msg)
	args.Txn.GoVec = buf
	var r shared.Status
	args.Txn.FileSize = uint64(len(args.File))
	err = cbc.Call("CNCBotCommunication.CompleteTransaction", args.Txn, &r)
	if err != nil {
		// So if we have an error here, we have no idea of knowing whether the CNC crashed OR just that there was a network failure.
		// The conservative thing would be to keep the file and verify it on restart
		msg.Content = "Failed to reach CNC txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = -1
		reply.Message = msg.Content
		shared.CheckErrorNonFatal("", err)
		return nil
	}
	Logger.UnpackReceive("Received reply from CNC for txid= "+tidstr, r.GoVec, &msg)

	if r.Success == 0 {
		msg.Content = r.Message + " txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = msg.Content
		removeTransactionFromUnverifiedReceived(args.Txn.TransactionID)
		// we should get rid of the file here because cnc said it's not valid
		os.Remove(fp)
		return nil
	}
	// We got this far so we know that we received a success from the CNC

	dberr = markUnverifiedReceivedAsCompleted(args.SenderAddress, args.Txn.TransactionID)
	if dberr != nil {
		msg.Content = dberr.Error() + " txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		shared.CheckErrorNonFatal("DB error: ", dberr)
		reply.Success = 1
		reply.Message = msg.Content
		return nil
	}
	msg.Content = "Transaction successful txid= " + tidstr
	msg.RealTimestamp = time.Now().String()
	buf = Logger.PrepareSend(msg.Content, msg)
	reply.GoVec = buf
	reply.Success = 1
	reply.Message = msg.Content
	return nil
}
func (b2b *Bot2BotCommunication) DeleteFile(args *shared.Transaction, reply *shared.Status) error {
	splitFilename := strings.Split(args.FileName, "|")
	nodeID := splitFilename[1]
	fp := filepath.Join("storedfiles", nodeID, args.FileName)
	tidstr := strconv.FormatUint(args.TransactionID, 10)

	var msg shared.Msg
	dberr := addToUnverifiedReceivedDeletions(*args)
	if dberr != nil {
		msg.Content = dberr.Error()
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(dberr.Error(), msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = "Could not update database for unverified received"
		return nil

	}
	Logger.UnpackReceive("Deletion txid= "+tidstr+" is marked as unverified", args.GoVec, &msg)

	cbc, err := rpc.Dial("tcp", cncrpcip)
	if err != nil {
		// CNC is down or network failure occured,
		msg.Content = err.Error() + " txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = "Could not reach CNC, file was not deleted"
		return nil
	}

	msg.Content = "Verifying deletion with CNC txid= " + tidstr
	msg.RealTimestamp = time.Now().String()
	buf := Logger.PrepareSend(msg.Content, msg)
	args.GoVec = buf
	var r shared.Status
	err = cbc.Call("CNCBotCommunication.CompleteDeletion", args, &r)
	if err != nil {
		// So if we have an error here, we have no idea of knowing whether the CNC crashed OR just that there was a network failure.
		// The conservative thing would be to keep the file and verify it on restart
		msg.Content = "Failed to reach CNC txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = -1
		reply.Message = "Could not verify transaction, will check with CNC"
		shared.CheckErrorNonFatal("", err)
		return nil
	}
	Logger.UnpackReceive("Received reply from CNC for txid= "+tidstr, r.GoVec, &msg)

	if r.Success == 0 {
		// We keep the file
		msg.Content = r.Message + " txid= " + tidstr
		msg.RealTimestamp = time.Now().String()
		buf := Logger.PrepareSend(msg.Content, msg)
		reply.GoVec = buf
		reply.Success = 0
		reply.Message = msg.Content
		return nil
	}
	markUnverifiedReceivedDeletionAsCompleted(args.SendingIP, args.TransactionID)
	os.Remove(fp)
	msg.Content = "Deletion transaction successful txid= " + tidstr
	msg.RealTimestamp = time.Now().String()
	buf = Logger.PrepareSend(msg.Content, msg)
	reply.GoVec = buf
	reply.Success = 1
	reply.Message = msg.Content

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// The CNC calls this to notify of a dead node
////////////////////////////////////////////////////////////////////////////////
func (c2b *CNCBotCommunication) DeadNodeNotification(args *shared.DeadNodeNotification, reply *[]uint64) error {
	if args.ZorY == "Z" { //Check which milestone the dead node has reached
		dbMutex.Lock()
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("PeerStoredFiles"))
			bfrs := b.Get([]byte(args.NodeIP))
			if bfrs == nil {
				return nil
			} else {
				frs, _ := decodeFrs(bfrs)
				for _, fr := range frs {
					splitFilename := strings.Split(fr.FileName, "|")
					nodeID := splitFilename[1]
					fileloc := filepath.Join("storedfiles", nodeID, fr.FileName)
					fmt.Println("Deleting file: ", fileloc)
					*reply = append(*reply, fr.TransactionID)
					os.Remove(fileloc)
				}
				err := b.Delete([]byte(args.NodeIP))
				return err

			}

			return nil
		})
		dbMutex.Unlock()
	} else {
		fns := make([]string, 0)
		dbMutex.Lock()
		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("CloudStoredFiles"))
			it := b.Cursor()
			for k, v := it.First(); k != nil; k, v = it.Next() {
				fileName := string(k[:])
				txns, _ := decodeTxns(v)
				transactions := make([]shared.Transaction, 0)
				for _, txn := range txns {
					if txn.ReceiverIP == args.NodeIP {
						//First we have to extract the filepath from the filename (filename is 3 parts, split by |, 3rd part is filepath)
						splitFN := strings.Split(fileName, "|") //First split
						fileName := splitFN[2]                  //Now get the filename
						fns = append(fns, fileName)
						// send back all transactions we are removing so that the cnc can issue a refund
						*reply = append(*reply, txn.TransactionID)

					} else {
						transactions = append(transactions, txn)

					}

				}
				btxns, err := encodeTxns(transactions)
				if err != nil {
					return err
				}
				err = b.Put(k, btxns)
				return err

			}
			return nil
		})
		dbMutex.Unlock()
		//And finally re-replicate
		for _, fn := range fns {
			sendCommand(fn, 1)
		}
	}

	return nil
}
func (b2b *Bot2BotCommunication) CheckStatus(args *FileTransaction, reply *shared.Status) error {
	//fr := FileRequest{botInfo.NodeID, args.SenderAddress, args.FileName, args.Txn.TransactionID}
	dbMutex.RLock()
	dberr := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("PeerStoredFiles"))
		v := b.Get([]byte(args.SenderAddress))
		if v == nil {
			return errors.New("No files found from that address")
		} else {
			frs, _ := decodeFrs(v)
			for _, fr := range frs {
				if fr.FileName == args.FileName {
					return nil
				}
			}
		}
		return errors.New("File not found")
	})
	dbMutex.RUnlock()
	if dberr != nil {
		reply.Success = 0
		reply.Message = dberr.Error()
		return nil

	}
	reply.Success = 1
	reply.Message = "File is found"
	return nil
}
