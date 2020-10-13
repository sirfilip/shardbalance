package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirfilip/shardbalance"
)

type ShardCreateValidation struct {
	addr string
}

func (s ShardCreateValidation) Submit() error {
	addr := strings.TrimSpace(s.addr)
	if addr == "" {
		return errors.New("addr is required")
	}
	return nil
}

type ShardDestroyValidation struct {
	addr string
}

func (s ShardDestroyValidation) Submit() error {
	addr := strings.TrimSpace(s.addr)
	if addr == "" {
		return errors.New("addr is required")
	}
	return nil
}

func createServer(capacity int64) *gin.Engine {
	r := gin.Default()

	balancer := shardbalance.New(capacity)

	r.GET("/:key", func(c *gin.Context) {
		addr, _, err := balancer.Addr(c.Param("key"))
		if err == nil {
			c.JSON(200, gin.H{"addr": addr})
			return
		}
		log.Println(err)
		c.JSON(404, gin.H{})
	})

	r.POST("/shard", func(c *gin.Context) {
		var err error

		form := ShardCreateValidation{addr: c.PostForm("address")}
		if err = form.Submit(); err == nil {
			err = balancer.Register(c.PostForm("address"))
			if err == nil {
				c.JSON(201, gin.H{})
				return
			}
		}
		c.JSON(400, gin.H{"errors": err})
	})

	r.DELETE("/shard", func(c *gin.Context) {
		var err error

		form := ShardDestroyValidation{c.PostForm("address")}
		if err = form.Submit(); err == nil {
			if err = balancer.Deregister(c.PostForm("address")); err == nil {
				c.JSON(200, gin.H{})
				return
			}
		}
		log.Println(err)
		c.JSON(400, gin.H{"errors": err})
	})

	return r
}

func main() {
	capacity := flag.Int64("capacity", 42, "shard key capacity")
	flag.Parse()

	createServer(*capacity).Run()
}
