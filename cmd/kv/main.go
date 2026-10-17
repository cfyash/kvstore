package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/cfyash/kvstore"
	"github.com/cfyash/kvstore/server"
)

const port = "127.0.0.1:6380"
const dataDir = "data"

func main() {
	db, err := kvstore.Open(dataDir)
	if err != nil {
		log.Fatalf("server startup failed %v\n", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	s := server.NewServer(db)
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("server startup failed %v\n", err)
	}
	log.Printf("listening on %s\n", port)
	var errs error
	if err := s.Serve(ctx, listener); err != nil {
		log.Printf("Server stopped with error: %v\n", err)
		errs = errors.Join(errs, err)
	} else {
		log.Println("Server stopped cleanly.")
	}

	log.Println("Shutting down database...")
	if err := db.Close(); err != nil {
		log.Printf("Error closing database: %v\n", err)
		errs = errors.Join(errs, err)
	}

	if errs != nil {
		log.Printf("Shutdown completed with errors: %v\n", errs)
		os.Exit(1)
	}
	log.Printf("Shutdown completed")
}
