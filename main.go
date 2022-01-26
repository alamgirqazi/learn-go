package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type empData struct {
	Name string
	Age  string
	City string
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Some error occured. Err: %s", err)
	}

	readCSVLocal()
	sftpDownload()

}

func readCSVLocal() {
	fmt.Println("reading CSV File from local")
	csvFile, err := os.Open("test.csv")
	if err != nil {
		fmt.Println(err)
		fmt.Println("cannot open the file ")
	}
	fmt.Println("Successfully Opened CSV file")
	defer csvFile.Close()

	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	for _, line := range csvLines {
		fmt.Println("linr ", line)
		// emp := empData{
		// 	Name: line[0],
		// 	Age:  line[1],
		// 	City: line[2],
		// }
		// fmt.Println(emp.Name + " " + emp.Age + " " + emp.City)
	}
}

func sftpDownload() {

	host := os.Getenv("HOST")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	client, err := connectToHost(user, host, password)
	if err != nil {
		panic(err)
	}

	// open an SFTP session over an existing ssh connection.
	clientSftp, err := sftp.NewClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// walk a directory
	w := clientSftp.Walk("/home/ta/files/")
	for w.Step() {
		if w.Err() != nil {
			continue
		}

		path := w.Path()

		newpath := filepath.Join(".", "tmp")
		err := os.MkdirAll(newpath, os.ModePerm)
		if err != nil {
			fmt.Println("err", err)
		}
		isCsv := strings.Contains(path, ".csv")
		timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
		localPath := "tmp/" + timestamp + ".csv"
		if isCsv {

			srcFile, err := clientSftp.OpenFile(path, (os.O_RDONLY))
			if err != nil {
				panic(err)
			}

			dstFile, err := os.Create(localPath)
			if err != nil {
				panic(err)
			}
			bytes, err := io.Copy(dstFile, srcFile)
			if err != nil {
				panic(err)
			}
			log.Printf("%d bytes copied to %v", bytes, localPath)
			defer dstFile.Close()
			defer srcFile.Close()

		}
	}

}

func connectToHost(user, host, pass string) (*ssh.Client, error) {

	sshConfig := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{ssh.Password(pass)},
	}
	sshConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()

	client, err := ssh.Dial("tcp", host, sshConfig)
	if err != nil {
		return nil, err
	}

	return client, nil
}
