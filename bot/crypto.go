package bot

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

func encryptFile(file []byte) []byte {
	//fmt.Printf("[encryptFile] encrypting file\n")
	key := []byte(BOTKEY)

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	ciphertext := make([]byte, aes.BlockSize+len(file))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], file)

	return ciphertext
}

func decrypt(ciphertext []byte) []byte {
	key := []byte(BOTKEY)

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	if len(ciphertext) < aes.BlockSize {
		panic("ciphertext too short")
	}
	// get iv out of the ciphertext
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	// decrypt
	stream := cipher.NewCFBDecrypter(block, iv)

	// XORKeyStream can work in-place if the two arguments are the same.
	stream.XORKeyStream(ciphertext, ciphertext)
	fmt.Printf("%s", ciphertext)
	return ciphertext
}
