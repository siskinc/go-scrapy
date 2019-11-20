package worker

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type MysqlInfo struct {
	Username string
	Password string
	Host     string
	Database string
	Table    string
	Port     string
}

type DatabaseWriterWorker struct {
	mysqlInfo                *MysqlInfo
	sqlFormat                string
	SqlInfoChan              chan []interface{}
	sqlInfoCache             [][]interface{}
	sqlInfoCacheIndex        int
	WorkerNumber             int
	WriteToDatabaseLen       int
	WriteToDatabaseTickerLen time.Duration
	locker                   sync.Mutex
	db                       *sql.DB
}

func NewDatabaseWorker(mysqlInfo *MysqlInfo, filedIndex []string, workerNumber int, writeToDbLen int, writeToDbTicker time.Duration, sqlChanNumber int) *DatabaseWriterWorker {
	if nil == mysqlInfo {
		panic("mysqlInfo is nil")
	}
	var err error
	worker := &DatabaseWriterWorker{}
	worker.db, err = sql.Open("mysql",
		fmt.Sprintf("%s:%s@(%s:%s)/%s",
			mysqlInfo.Username, mysqlInfo.Password, mysqlInfo.Host, mysqlInfo.Port, mysqlInfo.Database))
	if nil != err {
		panic(err)
	}
	worker.mysqlInfo = mysqlInfo
	worker.SetFiledIndex(filedIndex)
	worker.WorkerNumber = workerNumber
	worker.WriteToDatabaseLen = writeToDbLen
	worker.WriteToDatabaseTickerLen = writeToDbTicker
	worker.SqlInfoChan = make(chan []interface{}, sqlChanNumber)
	worker.sqlInfoCache = make([][]interface{}, writeToDbLen*2)
	return worker
}

func (worker *DatabaseWriterWorker) SetFiledIndex(fieldIndex []string) {
	database := worker.mysqlInfo.Database
	table := worker.mysqlInfo.Table
	fieldIndexStr := bytes.NewBufferString("(")
	fieldEmptyStr := bytes.NewBufferString("(")
	for i := range fieldIndex {
		if 0 != i {
			fieldIndexStr.WriteString(",")
			fieldEmptyStr.WriteString(",")
		}
		fieldIndexStr.WriteString(fieldIndex[i])
		fieldEmptyStr.WriteString("?")
	}
	fieldIndexStr.WriteString(")")
	fieldEmptyStr.WriteString(")")
	worker.sqlFormat = fmt.Sprintf("REPLACE INTO %s.%s %s VALUES %s",
		database, table, fieldIndexStr, fieldEmptyStr)
}

func (worker *DatabaseWriterWorker) Start() {
	ticker := time.NewTicker(worker.WriteToDatabaseTickerLen)
	for {
		select {
		case data := <-worker.SqlInfoChan:
			if worker.sqlInfoCacheIndex >= worker.WriteToDatabaseLen {
				worker.Do()
				worker.sqlInfoCacheIndex = 0
			}
			worker.sqlInfoCache[worker.sqlInfoCacheIndex] = data
			worker.sqlInfoCacheIndex++
		case <-ticker.C:
			logrus.Infof("database writer worker ticker")
			worker.Do()
			worker.sqlInfoCacheIndex = 0
		}
	}
}

func (worker *DatabaseWriterWorker) Do() {
	worker.locker.Lock()
	defer worker.locker.Unlock()
	if worker.sqlInfoCacheIndex <= 0 {
		return
	}
	tx, err := worker.db.Begin()
	if nil != err {
		logrus.Errorf("db.Begin is err: %v", err)
		return
	}
	for i := 0; i < worker.sqlInfoCacheIndex; i++ {
		_, err := tx.Exec(worker.sqlFormat, worker.sqlInfoCache[i]...)
		if nil != err {
			logrus.Errorf("write to db is err: %v, sqlFormat: %s, caches: %+v", err, worker.sqlFormat, worker.sqlInfoCache[i])
			continue
		}
	}
	err = tx.Commit()
	if nil != err {
		logrus.Errorf("commit is err: %v", err)
		return
	}
}
