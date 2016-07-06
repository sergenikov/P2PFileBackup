package bot

import (
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"net/rpc"
	"os"
	"path/filepath"
	"project_h2b9_s8z8_y4n8/shared"
	"strconv"
	"strings"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// Ask the CNC for a list of nodes which we can replicate on
////////////////////////////////////////////////////////////////////////////////
func requestReplication(args *shared.ReplicationRequest) (shared.ReplicationInfo, error) {
	// Connect to the CNCs RPC service.
	cbc, err := rpc.Dial("tcp", cncrpcip)
	var repinfo shared.ReplicationInfo //Placeholder for the CNC reply
	if err != nil {
		return repinfo, err
	}
	msg := shared.Msg{"Requesting Transaction", time.Now().String()}
	buf := Logger.PrepareSend("Requesting Transaction", msg)
	args.GoVec = buf
	err = cbc.Call("CNCBotCommunication.GetNodes", args, &repinfo) //Query the CNC
	if err != nil {
		return repinfo, err
	}
	if repinfo.Status.Success == 0 {
		return repinfo, errors.New(repinfo.Status.Message)
	}
	return repinfo, nil
}

////////////////////////////////////////////////////////////////////////////////
// Transfer a file to another bot. Needs the address of the other bot and the FileTransaction struct
////////////////////////////////////////////////////////////////////////////////
func replicateOnBot(file []byte, txn shared.Transaction, fn string) {
	botAddr := txn.ReceiverIP

	ftxn := FileTransaction{file, botInfo.IPport, botInfo.NodeID, fn, txn, make([]byte, 0)}
	tidstr := strconv.FormatUint(txn.TransactionID, 10)

	//First connect to the other bots RPC service
	conn, err := rpc.Dial("tcp", botAddr)
	if err != nil {
		shared.CheckErrorNonFatal("Your file was not replicated: ", err)
		return
	}

	dberr := addtoUnverifiedSentDB(txn)
	if dberr != nil {
		// Could not update database so we should return
		shared.CheckErrorNonFatal("Dberr: ", dberr)
		return
	}
	//Now attempt to transfer the file
	msg := shared.Msg{"Sending file txid= " + tidstr + " to " + botAddr, time.Now().String()}
	buf := Logger.PrepareSend(msg.Content, msg)
	ftxn.GoVec = buf
	var reply shared.Status
	err = conn.Call("Bot2BotCommunication.StoreFile", &ftxn, &reply) //Query the bot
	if err != nil {
		// Dial the CNC to check if the file was verified or not
		// TODO dunno what to do here with go vec
		Logger.LogLocalEvent("Connection lost with receiving node, dialing CNC txid= " + tidstr)
		verifySentTxnWithCNC(txn)
		return
	}

	fmt.Printf("Reply: %+v, %s\n", reply.Success, reply.Message)

	if reply.Success == 0 {
		Logger.UnpackReceive("Transaction rejected txid= "+tidstr, reply.GoVec, &msg)
		removeTransactionFromUnverifiedSent(txn.TransactionID)
		fmt.Println("Your file was not replicated: ", reply.Message)
		return
	} else if reply.Success == -1 {
		Logger.LogLocalEvent("Connection lost with C&C, dialing C&C txid= " + tidstr)
		verifySentTxnWithCNC(txn)

	} else {
		Logger.UnpackReceive("Transaction success txid= "+tidstr, reply.GoVec, &msg)
		markUnverifiedSentAsCompleted(fn, txn.TransactionID)

	}

}

////////////////////////////////////////////////////////////////////////////////
// Deletes a file to another bot. Needs the address of the other bot and the filename of the file
////////////////////////////////////////////////////////////////////////////////
func fileDeletion(txn shared.Transaction) {
	cbc, err := rpc.Dial("tcp", cncrpcip)
	tidstr := strconv.FormatUint(txn.TransactionID, 10)
	var rep shared.Status //Placeholder for the CNC reply
	if err != nil {
		shared.CheckErrorNonFatal("Request Deletion: ", err)
		return
	}
	msg := shared.Msg{"Requesting Deletion", time.Now().String()}
	buf := Logger.PrepareSend("Requesting Deletion", msg)
	txn.GoVec = buf
	err = cbc.Call("CNCBotCommunication.RequestDeletion", txn, &rep) //Query the CNC
	if err != nil {
		shared.CheckErrorNonFatal("Request Deletion: ", err)
		return
	}
	if rep.Success == 0 {
		fmt.Println("Request for deletion was denied: ", rep.Message)
	} else {
		botAddr := txn.ReceiverIP
		bot, err := rpc.Dial("tcp", botAddr)
		addToUnverifiedSentDeletions(txn)
		if err != nil {
			shared.CheckErrorNonFatal("Could not delete file: ", err)
		}
		msg := shared.Msg{"Sending deletion request txid= " + tidstr + " to " + botAddr, time.Now().String()}
		buf := Logger.PrepareSend(msg.Content, msg)
		txn.GoVec = buf
		err = bot.Call("Bot2BotCommunication.DeleteFile", txn, &rep)

		Logger.UnpackReceive("Received reply from "+botAddr+" for txid= "+tidstr, rep.GoVec, &msg)
		if rep.Success == 1 {
			fmt.Println("Your file was successfully deleted")
			markUnverifiedSentDeletionAsCompleted(txn.FileName, txn.TransactionID)
		} else {
			fmt.Println("Your file was not deleted")
		}
	}
	return

}

////////////////////////////////////////////////////////////////////////////////
// Retrieve a file to another bot. Needs the address of the other bot and the FileRequest struct
////////////////////////////////////////////////////////////////////////////////
func retrieveFileFromBot(botAddr string, request *FileRequest) {
	//First connect to the other bots RPC service
	conn, err := rpc.Dial("tcp", botAddr)
	shared.CheckErrorNonFatal("", err)
	//Now attempt to transfer the file
	var reply FileTransaction
	err = conn.Call("Bot2BotCommunication.RetrieveFile", request, &reply) //Query the bot
	shared.CheckErrorNonFatal("", err)
}

////////////////////////////////////////////////////////////////////////////////
// Send a heartbeat to the CNC
////////////////////////////////////////////////////////////////////////////////
func heartbeat() {
	hearbeatFrequency := 5 //The frequency of a heartbeat in seconds
	firstBeat := true
	for {
		cbc, err := rpc.Dial("tcp", cncrpcip)
		firstBeat = true //If we set firstBeat to true, the CNC will readd our details to the map when it recovers
		for {
			hbInfo := shared.Heartbeat{NodeInfo: &botInfo, FirstBeat: firstBeat}
			if err != nil {
				fmt.Println("Hearbeat failed!")
				break
			} else {
				err = cbc.Call("CNCBotCommunication.Heartbeat", &hbInfo, &hbInfo) //Query the CNC
				if err != nil {
					shared.CheckErrorNonFatal("", err)
					break
				}
				firstBeat = false
			}
			time.Sleep(time.Duration(hearbeatFrequency) * time.Second)
		}
		time.Sleep(time.Duration(hearbeatFrequency) * time.Second)
	}
}

func removeTransactionFromUnverifiedSent(tid uint64) {
	// delete file from database because it did not get replicated
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentTxns"))
		tidstr := strconv.FormatUint(tid, 10)
		err := b.Delete([]byte(tidstr))
		return err

	})
	dbMutex.Unlock()
}

func markUnverifiedSentAsCompleted(fn string, tid uint64) {
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("UnverifiedSentTxns"))
		tidstr := strconv.FormatUint(tid, 10)
		btxn := b.Get([]byte(tidstr))

		cb := tx.Bucket([]byte("CloudStoredFiles"))
		cbtxns := cb.Get([]byte(fn))
		txn, err := shared.DecodeTxn(btxn)
		if err != nil {
			return err
		}
		if cbtxns == nil {
			txns := make([]shared.Transaction, 0)
			txns = append(txns, txn)
			txnbytes, _ := encodeTxns(txns)
			err = cb.Put([]byte(fn), txnbytes)

		} else {

			ctxns, err := decodeTxns(cbtxns)
			if err != nil {
				return err
			}
			ctxns = append(ctxns, txn)
			txnbytes, err := encodeTxns(ctxns)
			err = cb.Put([]byte(fn), txnbytes)
		}
		if err != nil {
			return err

		}
		err = b.Delete([]byte(tidstr))
		return err
	})
	dbMutex.Unlock()
}
func addtoUnverifiedSentDB(txn shared.Transaction) error {
	//Add to unverified transactions database
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentTxns"))
		tidstr := strconv.FormatUint(txn.TransactionID, 10)
		enctxn, err := shared.EncodeTxn(&txn)
		if err != nil {
			return err
		}
		err = b.Put([]byte(tidstr), enctxn)
		return err
	})
	dbMutex.Unlock()
	return dberr

}

func removeTransactionFromUnverifiedReceived(tid uint64) {
	// delete file from database because it did not get replicated
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedTxns"))
		tidstr := strconv.FormatUint(tid, 10)
		err := b.Delete([]byte(tidstr))
		return err

	})
	dbMutex.Unlock()
}

func markUnverifiedReceivedAsCompleted(ip string, tid uint64) error {
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedTxns"))
		tidstr := strconv.FormatUint(tid, 10)
		btxn := b.Get([]byte(tidstr))

		fb := tx.Bucket([]byte("PeerStoredFiles"))
		fbtxns := fb.Get([]byte(ip))

		txn, err := shared.DecodeTxn(btxn)
		if err != nil {
			return err
		}
		fr := FileRequest{botInfo.NodeID, txn.SendingIP, txn.FileName, txn.TransactionID}
		if fbtxns == nil {
			frs := make([]FileRequest, 0)
			frs = append(frs, fr)
			frsbytes, _ := encodeFrs(frs)
			err = fb.Put([]byte(ip), frsbytes)

		} else {

			frs, err := decodeFrs(fbtxns)
			if err != nil {
				return err
			}
			frs = append(frs, fr)
			frsbytes, err := encodeFrs(frs)
			err = fb.Put([]byte(ip), frsbytes)
		}
		if err != nil {
			return err
		}
		err = b.Delete([]byte(tidstr))
		return err
	})

	dbMutex.Unlock()
	return dberr
}
func addToUnverifiedReceivedDB(txn shared.Transaction) error {
	//Add to unverified transactions database
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedTxns"))
		tidstr := strconv.FormatUint(txn.TransactionID, 10)
		enctxn, err := shared.EncodeTxn(&txn)
		if err != nil {
			return err
		}
		err = b.Put([]byte(tidstr), enctxn)
		return err
	})
	dbMutex.Unlock()
	return dberr
}

func verifySentTxnWithCNC(txn shared.Transaction) {
	tidstr := strconv.FormatUint(txn.TransactionID, 10)
	cbc, err := rpc.Dial("tcp", cncrpcip)
	msg := shared.Msg{"Verifying Sent Txn", time.Now().String()}
	buf := Logger.PrepareSend("Verifying Sent Txn txid= "+tidstr, msg)
	txn.GoVec = buf
	if err != nil {
		// We could not reach CNC so we should just leave this block of the log open so on restart we can verify transaction
		Logger.LogLocalEvent("Could not verify sent txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify transaction: ", err)
		return
	}
	// call the CNC to verify
	var r shared.Status
	err = cbc.Call("CNCBotCommunication.VerifyTransaction", txn, &r)
	if err != nil {
		// Could not get verification from CNC because of network error or crash just leave log block open
		Logger.LogLocalEvent("Could not verify sent txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify transaction: ", err)
		return

	}
	if r.Success == 0 {
		Logger.UnpackReceive("Sent transaction rejected txid= "+tidstr, r.GoVec, &msg)
		removeTransactionFromUnverifiedSent(txn.TransactionID)
		fmt.Println("Your file was not replicated")
		return
	} else {
		Logger.UnpackReceive("Sent transaction confirmed txid= "+tidstr, r.GoVec, &msg)
		markUnverifiedSentAsCompleted(txn.FileName, txn.TransactionID)
		fmt.Println("Your file was successfully replicated")

	}
}

func verifyReceivedTxnWithCNC(txn shared.Transaction) {
	splitFilename := strings.Split(txn.FileName, "|")
	nodeID := splitFilename[1]
	fp := filepath.Join("storedfiles", nodeID, txn.FileName)
	tidstr := strconv.FormatUint(txn.TransactionID, 10)
	msg := shared.Msg{"Verifying Received Txn", time.Now().String()}
	buf := Logger.PrepareSend("Verifying Received Txn txid= "+tidstr, msg)
	txn.GoVec = buf
	cbc, err := rpc.Dial("tcp", cncrpcip)
	if err != nil {
		// We could not reach CNC so we should just leave this block of the log open so on restart we can verify transaction
		Logger.LogLocalEvent("Could not verify received txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify transaction: ", err)
		return
	}
	// call the CNC to verify
	var r shared.Status
	err = cbc.Call("CNCBotCommunication.VerifyTransaction", txn, &r)
	if err != nil {
		// Could not get verification from CNC because of network error or crash just leave log block open
		Logger.LogLocalEvent("Could not verify received txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify transaction: ", err)
		return

	}
	if r.Success == 0 {
		Logger.UnpackReceive("Received transaction rejected txid= "+tidstr, r.GoVec, &msg)
		removeTransactionFromUnverifiedReceived(txn.TransactionID)
		os.Remove(fp)
		fmt.Println("Your file was not replicated")
	} else {
		Logger.UnpackReceive("Received transaction confirmed txid= "+tidstr, r.GoVec, &msg)
		markUnverifiedReceivedAsCompleted(txn.SendingIP, txn.TransactionID)
	}
	return
}

func verifyDeletedSentTxnWithCNC(txn shared.Transaction) {
	tidstr := strconv.FormatUint(txn.TransactionID, 10)
	cbc, err := rpc.Dial("tcp", cncrpcip)
	msg := shared.Msg{"Verifying Deletion", time.Now().String()}
	buf := Logger.PrepareSend("Verifying Deletion Txn txid= "+tidstr, msg)
	txn.GoVec = buf
	if err != nil {
		// We could not reach CNC so we should just leave this block of the log open so on restart we can verify transaction
		Logger.LogLocalEvent("Could not verify sent deletion txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify deletion: ", err)
		return
	}
	// call the CNC to verify
	var r shared.Status
	err = cbc.Call("CNCBotCommunication.VerifyDeletion", txn, &r)
	if err != nil {
		// Could not get verification from CNC because of network error or crash just leave log block open
		Logger.LogLocalEvent("Could not verify sent deletion txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify deletion: ", err)
		return

	}
	if r.Success == 0 {
		Logger.UnpackReceive("Deletion rejected txid= "+tidstr, r.GoVec, &msg)
		removeFromUnverifiedSentDeletions(txn.TransactionID)
		fmt.Println("File is not deleted")
	} else {
		Logger.UnpackReceive("Deletion confirmed txid= "+tidstr, r.GoVec, &msg)
		markUnverifiedSentDeletionAsCompleted(txn.FileName, txn.TransactionID)
	}
	return
}

func verifyDeletedReceivedTxnWithCNC(txn shared.Transaction) {
	splitFilename := strings.Split(txn.FileName, "|")
	nodeID := splitFilename[1]
	fp := filepath.Join("storedfiles", nodeID, txn.FileName)
	tidstr := strconv.FormatUint(txn.TransactionID, 10)
	msg := shared.Msg{"Verifying Deletion", time.Now().String()}
	buf := Logger.PrepareSend("Verifying Deletion Txn txid= "+tidstr, msg)
	txn.GoVec = buf
	cbc, err := rpc.Dial("tcp", cncrpcip)
	if err != nil {
		// We could not reach CNC so we should just leave this block of the log open so on restart we can verify transaction
		Logger.LogLocalEvent("Could not verify received deletion txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify deletion: ", err)
		return
	}
	// call the CNC to verify
	var r shared.Status
	err = cbc.Call("CNCBotCommunication.VerifyDeletion", txn, &r)
	if err != nil {
		// Could not get verification from CNC because of network error or crash just leave log block open
		Logger.LogLocalEvent("Could not verify received deletion txn txid= " + tidstr + " with CNC")
		shared.CheckErrorNonFatal("Unable to verify deletion: ", err)
		return

	}
	if r.Success == 0 {
		Logger.UnpackReceive("Deletion rejected txid= "+tidstr, r.GoVec, &msg)
		removeFromUnverifiedReceivedDeletions(txn.TransactionID)
		fmt.Println("File is not deleted")
	} else {
		Logger.UnpackReceive("Deletion confirmed txid= "+tidstr, r.GoVec, &msg)
		markUnverifiedReceivedDeletionAsCompleted(txn.SendingIP, txn.TransactionID)
		os.Remove(fp)
	}
	return
}

func removeFromUnverifiedSentDeletions(tid uint64) {
	// delete file from database because it did not get replicated
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentDeletions"))
		tidstr := strconv.FormatUint(tid, 10)
		err := b.Delete([]byte(tidstr))
		return err

	})
	dbMutex.Unlock()
}

func addToUnverifiedSentDeletions(txn shared.Transaction) error {
	//Add to unverified transactions database
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedSentDeletions"))
		tidstr := strconv.FormatUint(txn.TransactionID, 10)
		enctxn, err := shared.EncodeTxn(&txn)
		if err != nil {
			return err
		}
		err = b.Put([]byte(tidstr), enctxn)
		return err
	})
	dbMutex.Unlock()
	return dberr
}
func removeFromUnverifiedReceivedDeletions(tid uint64) {
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedDeletions"))
		tidstr := strconv.FormatUint(tid, 10)
		err := b.Delete([]byte(tidstr))
		return err

	})
	dbMutex.Unlock()
}

func addToUnverifiedReceivedDeletions(txn shared.Transaction) error {
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("UnverifiedReceivedDeletions"))
		tidstr := strconv.FormatUint(txn.TransactionID, 10)
		enctxn, err := shared.EncodeTxn(&txn)
		if err != nil {
			return err
		}
		err = b.Put([]byte(tidstr), enctxn)
		return err
	})
	dbMutex.Unlock()
	return dberr
}

func markUnverifiedReceivedDeletionAsCompleted(ip string, tid uint64) error {
	dbMutex.Lock()
	dberr := db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("UnverifiedReceivedDeletions"))
		tidstr := strconv.FormatUint(tid, 10)
		err = b.Delete([]byte(tidstr))

		fb := tx.Bucket([]byte("PeerStoredFiles"))
		fbtxns := fb.Get([]byte(ip))

		if err != nil {
			return err
		}
		newfrs := make([]FileRequest, 0)
		if fbtxns == nil {
			return errors.New("File transaction not present")
		} else {
			frs, err := decodeFrs(fbtxns)
			if err != nil {
				return err
			}
			//fr := FileRequest{botInfo.NodeID, txn.SendingIP, txn.FileName, txn.TransactionID}
			for _, fr := range frs {
				if fr.TransactionID != tid {
					newfrs = append(newfrs, fr)
				}
			}
			if len(newfrs) == 0 {
				err = fb.Delete([]byte(ip))
				return err
			}
			frsbytes, err := encodeFrs(newfrs)
			err = fb.Put([]byte(ip), frsbytes)
		}
		if err != nil {
			return err
		}
		return nil
	})
	dbMutex.Unlock()
	return dberr
}

func markUnverifiedSentDeletionAsCompleted(fn string, tid uint64) {
	dbMutex.Lock()
	db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("UnverifiedSentDeletions"))
		tidstr := strconv.FormatUint(tid, 10)
		err = b.Delete([]byte(tidstr))
		if err != nil {
			return err
		}

		cb := tx.Bucket([]byte("CloudStoredFiles"))
		cbtxns := cb.Get([]byte(fn))
		if err != nil {
			return err
		}
		if cbtxns == nil {
			return errors.New("File was not in cloud storage")

		} else {
			newtxns := make([]shared.Transaction, 0)
			ctxns, err := decodeTxns(cbtxns)
			if err != nil {
				return err
			}
			for _, txn := range ctxns {
				if txn.TransactionID != tid {
					newtxns = append(newtxns, txn)
				}

			}
			if len(newtxns) == 0 {
				err = cb.Delete([]byte(fn))
				return err
			}
			txnbytes, err := encodeTxns(newtxns)
			if err != nil {
				return err

			}
			err = cb.Put([]byte(fn), txnbytes)
		}
		return err
	})
	dbMutex.Unlock()
}
