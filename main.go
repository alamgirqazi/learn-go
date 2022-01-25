package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type empData struct {
	Name string
	Age  string
	City string
}

func main() {
	fmt.Println("hey there")
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
	sftpDownload()

}

func sftpDownload() {

	client, err := connectToHost("ta", "10.2.4.194:22", "Bingo#777")
	// "Bingo#777"
	if err != nil {
		panic(err)
	}

	// open an SFTP session over an existing ssh connection.
	clientSftp, err := sftp.NewClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// w := clientSftp.
	// walk a directory
	w := clientSftp.Walk("/home/ta/files/")
	for w.Step() {
		if w.Err() != nil {
			continue
		}

		// csvDirectory := w.Stat().IsDir()
		path := w.Path()
		isCsv := strings.Contains(path, ".csv")

		if isCsv {
			sftpDownload()
		}
		log.Println("isCsv", isCsv)
		downloadFile(client, path, "temp")
	}

	// check it's there
	// fi, err := clientSftp.Lstat("/home/ta/hello.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println(fi)
}

func downloadFile(sc sftp.Client, remoteFile, localFile string) (err error) {

	localPath := "/tmp/" + localFile

	log.Printf("Downloading [%s] to [%s] ...", remoteFile, localFile)
	// Note: SFTP To Go doesn't support O_RDWR mode
	srcFile, err := sc.OpenFile(remoteFile, (os.O_RDONLY))
	if err != nil {
		return fmt.Errorf("unable to open remote file: %v", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("unable to open local file: %v", err)
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("unable to download remote file: %v", err)
	}
	log.Printf("%d bytes copied to %v", bytes, localPath)

	return nil
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
