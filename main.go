package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/jiaxuanzhou/etcdV3Lock/util"
)

var (
	Key       string
	Mutex     = &concurrency.Mutex{}
	lockTTL   = 10
	StopCh    = make(chan struct{})
	stopTaskC = make(chan struct{})
)

func main() {
	Etcdv3Client, err := clientv3.New(clientv3.Config{
		Endpoints:            []string{"http://127.0.0.1:2379"},
		DialTimeout:          time.Second * 5,
		DialKeepAliveTime:    time.Second * 5,
		DialKeepAliveTimeout: time.Second * 6,
	})
	if err != nil {
		log.Fatalf("Failed to init etcdV3Client, err: %q", err)
	}

	util.Forever(func() {
		err := GainLock(Etcdv3Client)
		if err != nil {
			log.Printf("[WARNING] Check or Gain lock failed: %v", err)
		}
	}, time.Second*5)
}

func GainLock(cli *clientv3.Client) error {
	if cli == nil {
		log.Printf("[ERROR] etcd client is not initialed!")
		return errors.New("etcdv3 client is nil")
	}

	ctx, _ := context.WithTimeout(context.TODO(), time.Second*5)
	if _, err := cli.Get(ctx, "/"); err != nil {
		return fmt.Errorf("failed to connect to etcd, err: %v", err)
	}

	if Key == "" {
		s, err := concurrency.NewSession(cli, concurrency.WithTTL(lockTTL))
		if err != nil {
			return err
		}
		defer s.Close()
		Mutex = concurrency.NewMutex(s, "lock")
		if err := Mutex.Lock(context.TODO()); err != nil {
			return err
		}
	}

	k, kerr := cli.Get(ctx, Mutex.Key())
	if kerr != nil {
		return kerr
	}
	if len(k.Kvs) == 0 {
		return errors.New("lock lost on init")
	}

	key := k.Kvs[0]
	Key = string(key.Key)

	log.Printf("the key is %s", key)

	go func() {
		for {
			time.Sleep(time.Second * 30)
			ctx, _ := context.WithTimeout(context.TODO(), time.Second*5)
			k, kerr := cli.Get(ctx, Mutex.Key())
			if kerr != nil {
				log.Printf("the error is %v", kerr)
				StopCh <- struct{}{}
				return
			}
			if len(k.Kvs) == 0 {
				log.Printf("failed to get key of the lock ")
				StopCh <- struct{}{}
				return
			}

			key := k.Kvs[0]

			log.Printf("the lock key is %s", string(key.Key))

			if Key != "" && string(key.Key) != Key {
				StopCh <- struct{}{}
			}
		}
	}()
	log.Printf("[NORMAL] gained the lock, start doing something!")
	doSomething(StopCh)
	Key = ""
	return nil
}

func doSomething(stopC <-chan struct{}) {
	go util.Until(func() {
		fmt.Printf("this is a test for cuncurrency lock of etcd v3")
	}, time.Second*10, stopTaskC)

	<-stopC
	log.Printf("lock was released, stop the task")
	stopTaskC <- struct{}{}
}
