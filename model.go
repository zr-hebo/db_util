package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

const (
	dbTypeMysql = "mysql"
)

// Host  主机
type Host struct {
	IP     string `json:"ip"`
	Domain string `json:"domain"`
	Port   int    `json:"port"`
}

// UnanimityHost  id标示的主机
type UnanimityHost struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (uh *UnanimityHost) String() string {
	return fmt.Sprintf("%s:%d", uh.Host, uh.Port)
}

// UnanimityHostWithDomains   带域名的id标示的主机
type UnanimityHostWithDomains struct {
	UnanimityHost
	IP      string   `json:"ip"`
	Domains []string `json:"domains"`
}

// MysqlDB Mysql主机实例
type MysqlDB struct {
	Host
	UserName       string
	Passwd         string
	DatabaseType   string
	DBName         string
	ConnectTimeout int
}

// PooledMysqlDB Mysql主机实例
type PooledMysqlDB struct {
	MysqlDB
	conn *sql.DB
	lock *sync.Mutex
}

// NewPooledMysqlDBWithParam 带参数创建MySQL数据库
func NewPooledMysqlDBWithParam(
	ip string, port int, userName, passwd string) (
	pmd *PooledMysqlDB) {
	pmd = NewPooledMysqlDB()
	pmd.IP = ip
	pmd.Port = port
	pmd.UserName = userName
	pmd.Passwd = passwd
	pmd.DatabaseType = dbTypeMysql

	return
}

// NewPooledMysqlDBWithAllParam 带参数创建MySQL数据库
func NewPooledMysqlDBWithAllParam(
	ip string, port int, userName, passwd, dbName string) (
	pmd *PooledMysqlDB) {
	pmd = NewPooledMysqlDB()
	pmd.IP = ip
	pmd.Port = port
	pmd.UserName = userName
	pmd.Passwd = passwd
	pmd.DBName = dbName

	return
}

// NewPooledMysqlDB 创建MySQL数据库
func NewPooledMysqlDB() (pmd *PooledMysqlDB) {
	pmd = new(PooledMysqlDB)
	pmd.DatabaseType = dbTypeMysql
	pmd.lock = new(sync.Mutex)
	return
}

// NewMysqlDB 创建MySQL数据库
func NewMysqlDB() (md *MysqlDB) {
	md = new(MysqlDB)
	md.DatabaseType = dbTypeMysql
	return
}

// NewMysqlDBWithAllParam 带参数创建MySQL数据库
func NewMysqlDBWithAllParam(
	ip string, port int, userName, passwd, dbName string) (
	pmd *MysqlDB) {
	pmd = NewMysqlDB()
	pmd.IP = ip
	pmd.Port = port
	pmd.UserName = userName
	pmd.Passwd = passwd
	pmd.DBName = dbName

	return
}

// GetConnection 获取数据库连接
func (md *MysqlDB) GetConnection() (*sql.DB, error) {
	connStr := md.fillConnStr()

	stmtDB, err := sql.Open(md.DatabaseType, connStr)
	if err != nil {
		if stmtDB != nil {
			stmtDB.Close()
		}
		return nil, err
	}

	stmtDB.SetMaxOpenConns(0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := stmtDB.PingContext(ctx); err != nil {
		return nil, err
	}

	return stmtDB, nil
}

// CloseConnection 获取数据库连接
func (pmd *PooledMysqlDB) CloseConnection() (err error) {
	if pmd.conn == nil {
		return
	}

	err = pmd.conn.Close()
	return
}

// GetConnection 获取数据库连接
func (pmd *PooledMysqlDB) GetConnection() (conn *sql.DB, err error) {
	pmd.lock.Lock()
	defer func() {
		pmd.lock.Unlock()
	}()

	if pmd.conn != nil {
		conn = pmd.conn
		return
	}

	conn, err = pmd.MysqlDB.GetConnection()
	if err != nil {
		return
	}

	conn.SetConnMaxLifetime(0)
	conn.SetMaxOpenConns(0)
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	pmd.conn = conn

	return
}

// ExecChange 执行MySQL Query语句
func (pmd *PooledMysqlDB) ExecChange(stmt string, args ...interface{}) (
	result sql.Result, err error) {
	conn, err := pmd.GetConnection()
	if err != nil {
		return
	}

	result, err = conn.Exec(stmt, args...)
	return
}

// ExecQuery 执行MySQL Query语句
func (pmd *PooledMysqlDB) ExecQuery(stmt string) (rows *sql.Rows, err error) {
	conn, err := pmd.GetConnection()
	if err != nil {
		return
	}

	rows, err = conn.Query(stmt)
	return
}

// QueryRows 执行MySQL Query语句
func (pmd *PooledMysqlDB) QueryRows(stmt string) (rows *sql.Rows, err error) {
	conn, err := pmd.GetConnection()
	if err != nil {
		return
	}

	rows, err = conn.Query(stmt)
	return
}

// QueryRow 执行MySQL Query语句
func (pmd *PooledMysqlDB) QueryRow(stmt string) (row *sql.Row, err error) {
	conn, err := pmd.GetConnection()
	if err != nil {
		return
	}

	row = conn.QueryRow(stmt)
	return
}

// ExecQuery 执行MySQL Query语句
func (md *MysqlDB) ExecQuery(stmt string) (rows *sql.Rows, err error) {
	conn, err := md.GetConnection()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return
	}

	rows, err = conn.Query(stmt)
	return
}

// QueryRows 执行MySQL Query语句，返回多条数据
func (md *MysqlDB) QueryRows(stmt string) (rows *sql.Rows, err error) {
	conn, err := md.GetConnection()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return
	}

	rows, err = conn.Query(stmt)
	return
}

// QueryRow 执行MySQL Query语句，返回１条或０条数据
func (md *MysqlDB) QueryRow(stmt string) (row *sql.Row, err error) {
	conn, err := md.GetConnection()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return
	}

	row = conn.QueryRow(stmt)
	return
}

// ExecChange 执行MySQL Query语句
func (md *MysqlDB) ExecChange(stmt string, args ...interface{}) (
	result sql.Result, err error) {
	conn, err := md.GetConnection()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return
	}
	defer CloseConnection(conn)

	result, err = conn.Exec(stmt, args...)
	return
}

func (md *MysqlDB) fillConnStr() string {
	dbServerInfoStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		md.UserName, md.Passwd, md.IP, md.Port, md.DBName)
	if md.ConnectTimeout > 0 {
		dbServerInfoStr = fmt.Sprintf("%s?timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			dbServerInfoStr, md.ConnectTimeout, md.ConnectTimeout, md.ConnectTimeout)
	}

	return dbServerInfoStr
}
