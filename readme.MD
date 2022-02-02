# it ain't much and it doesn't work (me after spending hours on it)

### this is a simple Go consumer which reads files from a remote server (SFTP) and downloads the files locally and reads them and inserts them into Clickhouse. please create a `.env` file (copy from sample.env) before running this via `go run main.go`
#### What it does ?

> Read CSV File from a local Directory

> SFTP to another server and copy file to local server

> Uses Environment variables for credentials of SSH server 

> Uses goroutines and channels

> inserts csv records into Clickhouse

