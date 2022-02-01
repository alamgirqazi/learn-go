package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
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
	var start = time.Now()

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Some error occured. Err: %s", err)
	}

	sftpDownload()
	sftpTime := time.Now()
	fmt.Println("SFTP Time", time.Since(start))

	connectToClick := time.Now()
	conn, ctx, err__ := connectToClickhouse()

	fmt.Println("READ CSV Time", time.Since(sftpTime))
	readCSVLocal(conn, ctx)
	readCSVTime := time.Now()
	deleteLocalFiles()
	fmt.Println("Delete CSV Time", time.Since(readCSVTime))

	if err__ != nil {
		log.Fatalf("Some error occured. Err: %s", err)
	}
	fmt.Println("Connect to Clickhouse Time", time.Since(connectToClick))
	createTable := time.Now()

	// error_ := createClickhouseTable(conn, ctx)

	// if error_ != nil {

	// 	fmt.Println("ERROR")
	// }
	fmt.Println("Create Clickhouse Table", time.Since(createTable))

}

// generateCHCSVs(){

// }

func connectToClickhouse() (ch.Conn, context.Context, error) {

	host := os.Getenv("CH_HOST")
	user := os.Getenv("CH_USER")
	password := os.Getenv("CH_PASSWORD")

	conn, err := ch.Open(&ch.Options{
		Addr: []string{host},
		Auth: ch.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		//Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &ch.Compression{
			Method: ch.CompressionLZ4,
		},
		Settings: ch.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, nil, err

	}
	ctx := ch.Context(context.Background(), ch.WithSettings(ch.Settings{
		"max_block_size": 10,
	}), ch.WithProgress(func(p *ch.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*ch.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, nil, err
	}

	return conn, ctx, nil

}

func createClickhouseTable(conn ch.Conn, ctx context.Context) error {
	// err := conn.Exec(ctx, `DROP TABLE IF EXISTS example`)
	// if err != nil {
	// 	return err
	// }
	err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS example2 (
			Name String,
			Age String,
			City String
		) engine=Memory
	`)
	if err != nil {
		return err
	}
	return nil
}

func insertIntoCHTable(conn ch.Conn, ctx context.Context, empArray []empData) error {
	fmt.Println("inserting into CH ")
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example2 (Name, Age, City)")
	if err != nil {
		return err
	}
	for i := 0; i < len(empArray); i++ {

		name := empArray[i].Name
		age := empArray[i].Age
		city := empArray[i].City
		if err := batch.Append(name, age, city); err != nil {
			// if err := batch.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return err
	}
	return nil
}

func deleteLocalFiles() {
	localDirectory := "tmp"
	files, err := ioutil.ReadDir(localDirectory)

	if err != nil {

		log.Fatal(err)
	}

	for _, f := range files {
		name := localDirectory + "/" + f.Name()
		os.Remove(name)
	}
}

func insertToCH(c1 chan empData, c2 chan bool, wg *sync.WaitGroup, conn ch.Conn, ctx context.Context) {
	// est blish ch connection
	// forever loop
	var empArray []empData = nil
	// datastructure
	vr := true
	for vr {
		select {
		case msg1 := <-c1:
			empArray = append(empArray, msg1)
			if len(empArray)%5 == 0 {

				insertIntoCHTable(conn, ctx, empArray)
				empArray = nil
			}
			//appemd to data struct
			// if sizze == 100000
			// insert block
			// zero out datastructure
		case <-c2:
			// no more file readers
			// empty the channel c1
			// insert and kill
			fmt.Println("Received kill")
			if len(empArray) > 0 {
				insertIntoCHTable(conn, ctx, empArray)

				empArray = nil
			}
			vr = false
		}
	}
	defer wg.Done()
	defer fmt.Println("lol")

}

func readCSVLocal(conn ch.Conn, ctx context.Context) {
	fmt.Println("reading CSV File from local")
	localDirectory := "tmp"
	files, err := ioutil.ReadDir(localDirectory)

	if err != nil {

		log.Fatal(err)
	}

	var wgreader sync.WaitGroup
	var wginserter sync.WaitGroup

	// var basenameOpts []empData
	channel := make(chan empData, 10000)
	channel2 := make(chan bool, 10000)

	numberOfIserters := 5

	wginserter.Add(numberOfIserters)
	for i := 0; i < numberOfIserters; i++ {
		go insertToCH(channel, channel2, &wginserter, conn, ctx)
	}

	for _, f := range files {
		fmt.Println("file s")
		wgreader.Add(1)
		// fmt.Println(runtime.NumGoroutine())
		go reader(f, localDirectory, channel, channel2, &wgreader)

	}
	wgreader.Wait()
	fmt.Println("No more readers")
	// for i := 0; i < numberOfIserters; i++ {
	// 	channel2 <- true
	// }
	wginserter.Wait()
	fmt.Println("after ", runtime.NumGoroutine(), runtime.NumCPU(), runtime.NumCgoCall())

}

func reader(f fs.FileInfo, localDirectory string, c1 chan empData, c2 chan bool, wg *sync.WaitGroup) {

	// channel := make(chan []byte, 10000)
	name := localDirectory + "/" + f.Name()
	csvFile, err := os.Open(name)
	if err != nil {
		fmt.Println(err)
		fmt.Println("cannot open the file ")
	}

	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println(err)
	}

	for _, line := range csvLines {
		// fmt.Println("CSV ", line)
		emp := empData{
			Name: line[0],
			Age:  line[1],
			City: line[2],
		}

		c1 <- emp

	}
	fmt.Println("File completed")

	wg.Done()
	defer csvFile.Close()
}

// download files from SFTP Server

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

// download files and store to local
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

// connection to SSH
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
