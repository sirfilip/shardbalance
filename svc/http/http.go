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
	Addr string
}

func (s ShardCreateValidation) Submit() error {
	addr := strings.TrimSpace(s.Addr)
	if addr == "" {
		return errors.New("address is required")
	}
	return nil
}

type ShardDestroyValidation struct {
	Addr string
}

func (s ShardDestroyValidation) Submit() error {
	addr := strings.TrimSpace(s.Addr)
	if addr == "" {
		return errors.New("address is required")
	}
	return nil
}

func createServer(capacity int64) *gin.Engine {
	r := gin.Default()

	balancer := shardbalance.New(capacity)

	r.GET("/:key", func(c *gin.Context) {
		addr, _, err := balancer.Addr(c.Param("key"))
		if err == nil {
			c.JSON(200, gin.H{"address": addr})
			return
		}
		log.Println(err)
		c.JSON(404, gin.H{})
	})

	r.POST("/shards", func(c *gin.Context) {
		var err error

		form := ShardCreateValidation{c.PostForm("address")}
		if err = form.Submit(); err == nil {
			err = balancer.Register(form.Addr)
			if err == nil {
				c.JSON(201, gin.H{})
				return
			}
		}
		c.JSON(400, gin.H{"error": err.Error()})
	})

	r.DELETE("/shards/:address", func(c *gin.Context) {
		var err error

		form := ShardDestroyValidation{c.Param("address")}
		if err = form.Submit(); err == nil {
			if err = balancer.Deregister(form.Addr); err == nil {
				c.JSON(200, gin.H{})
				return
			}
		}
		log.Println(err)
		c.JSON(400, gin.H{"error": err.Error()})
	})

	return r
}

func main() {
	capacity := flag.Int64("capacity", 42, "shard key capacity")
	flag.Parse()

	createServer(*capacity).Run()
}
