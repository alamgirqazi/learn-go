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

func sftpDownload() error {

	host := os.Getenv("HOST")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	client, err := connectToHost(user, host, password)
	if err != nil {
		return fmt.Errorf("Some error occured")
	}

	// open an SFTP session over an existing ssh connection.
	clientSftp, err := sftp.NewClient(client)
	fmt.Printf("%T", clientSftp)

	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("Some error occured")
	}
	defer client.Close()

	newpath := filepath.Join(".", "tmp")
	errMkdir := os.MkdirAll(newpath, os.ModePerm)
	if errMkdir != nil {
		fmt.Println("err", err)
	}

	// walk a directory
	w := clientSftp.Walk("/home/ta/files/")
	for w.Step() {

		if w.Err() != nil {
			continue
		}

		path := w.Path()

		isCsv := strings.Contains(path, ".csv")

		if isCsv {

			downloadAndSave(clientSftp, path)

		}
	}
	return nil

}

// In case your function does not return anything, then return errors, dont cause a panic within the function

func downloadAndSave(clientSftp *sftp.Client, path string) error {
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	localPath := "tmp/" + timestamp + ".csv"
	srcFile, err := clientSftp.OpenFile(path, (os.O_RDONLY))
	if err != nil {
		return fmt.Errorf("Some error occured")
	}

	dstFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("Some error occured")
	}
	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("Some error occured")
	}
	log.Printf("%d bytes copied to %v", bytes, localPath)
	defer dstFile.Close()
	defer srcFile.Close()
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
