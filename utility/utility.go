package utility

import (
	"io"
	"net/url"
	"net/http"
	"bytes"
	"time"
	"fmt"
	"os"
	"encoding/base64"
	"crypto/sha256"
	"crypto/aes"
	"crypto/cipher"
	"io/ioutil"
	"golang.org/x/net/proxy"
)

func GetHttpResult(address string) (error, string) {
	u, err := url.Parse(address)
	if err != nil {
		return err, ""
	}
	res, err := http.Get(u.String())
	if err != nil {
		return err, ""
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err, ""
	}

	return nil, string(result)
}

func PostHttpResult(address string, body *bytes.Buffer) (error, []byte) {

	res, err := http.Post(address, "application/json;charset=utf-8", body)
	if err != nil {
		return err, nil
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err, nil
	}

	return nil, result
}

func PostHttpResultAdvanced(address string, proxyAddress string, header map[string]string, body *bytes.Buffer) (error, []byte) {
	// setup a http client

	timeout, _ := time.ParseDuration("30s")

	return HttpRequest("POST", address, proxyAddress, header, body, timeout)
	//httpTransport := &http.Transport{}
	//httpClient := &http.Client{Transport: httpTransport}
	//if proxyAddress != ""{
	//	dialer, err := proxy.SOCKS5("tcp", proxyAddress, nil, proxy.Direct)
	//	if err != nil {
	//		fmt.Fprintln(os.Stderr, "can't connect to the proxy:", err)
	//		os.Exit(1)
	//	}
	//	// set our socks5 as the dialer
	//	httpTransport.Dial = dialer.Dial
	//}
	//
	//req, err := http.NewRequest("POST", address, body)
	//if err != nil {
	//	return err, nil
	//}
	//
	//for k,v := range header{
	//	req.Header.Add(k, v)
	//}
	//
	//res,err := httpClient.Do(req)
	//if err != nil {
	//	return err, nil
	//}
	//
	//result, err := ioutil.ReadAll(res.Body)
	//res.Body.Close()
	//if err != nil {
	//	return err, nil
	//}
	//
	//
	//return nil, result
}

func GetHttpResultAdvanced(address string, proxyAddress string, header map[string]string) (error, []byte) {
	timeout, _ := time.ParseDuration("30s")
	return HttpRequest("GET", address, proxyAddress, header, nil, timeout)
}

func HttpRequest(method string, address string, proxyAddress string, header map[string]string, body *bytes.Buffer, timeout time.Duration) (error, []byte) {
	httpTransport := &http.Transport{}
	httpClient := &http.Client{Transport: httpTransport, Timeout: timeout}
	if proxyAddress != "" {
		dialer, err := proxy.SOCKS5("tcp", proxyAddress, nil, proxy.Direct)
		if err != nil {
			fmt.Fprintln(os.Stderr, "can't connect to the proxy:", err)
			os.Exit(1)
		}
		// set our socks5 as the dialer
		httpTransport.Dial = dialer.Dial
	}

	if body == nil {
		body = bytes.NewBufferString("")
	}

	req, err := http.NewRequest(method, address, body)
	if err != nil {
		return err, nil
	}

	if header != nil {
		for k, v := range header {
			req.Header.Add(k, v)
		}
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return err, nil
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err, nil
	}

	return err, result
}

func AesCBCDecrypter(inputText string, key []byte) (string, error) {
	//ciphertext, _ := hex.DecodeString(inputText)
	s, err := base64.StdEncoding.DecodeString(inputText)
	if err != nil {
		return "", err
	}
	ciphertext := []byte(s)

	sha := sha256.New()
	sha.Write(key)
	//	keyDigest := hex.EncodeToString(sha.Sum(nil))
	key = sha.Sum(nil)

	//	block, err := aes.NewCipher([]byte(keyDigest))
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(ciphertext) < aes.BlockSize {
		panic("ciphertext too short")
	}
	//iv := ciphertext[:aes.BlockSize]
	//ciphertext = ciphertext[aes.BlockSize:]

	// CBC mode always works in whole blocks.
	if len(ciphertext)%aes.BlockSize != 0 {
		panic("ciphertext is not a multiple of the block size")
	}

	var iv [aes.BlockSize]byte

	mode := cipher.NewCBCDecrypter(block, iv[:aes.BlockSize])

	// CryptBlocks可以原地更新
	dstBuff := make([]byte, len(ciphertext))
	mode.CryptBlocks(dstBuff, ciphertext)

	dstBuff = PKCS5UnPadding(dstBuff)

	result := string(dstBuff)
	return result, nil
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// copyBuffer is the actual implementation of Copy and CopyBuffer.
// if buf is nil, one is allocated.
func CopyBuffer(cancel chan int, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.

	go func() {
		<-cancel
		if c, ok := dst.(io.WriteCloser); ok {
			c.Close()
		}

		if c, ok := src.(io.ReadCloser); ok {
			c.Close()
		}
	}()

	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}
	if buf == nil {
		buf = make([]byte, 32*1024)
	}
	for {
		select {
		case <-cancel:
			return written, err
		default:
			nr, er := src.Read(buf)
			if nr > 0 {
				nw, ew := dst.Write(buf[0:nr])
				if nw > 0 {
					written += int64(nw)
				}
				if ew != nil {
					err = ew
					break
				}
				if nr != nw {
					err = io.ErrShortWrite
					break
				}
			}
			if er != nil {
				if er != io.EOF {
					err = er
				}
				break
			}
		}

	}
	return written, err
}

func StreamToByte(stream io.Reader) []byte {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Bytes()
}

func StreamToString(stream io.Reader) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.String()
}