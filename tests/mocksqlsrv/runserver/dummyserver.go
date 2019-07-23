package main

import (
     "log"
     "os"
     "github.com/paypal/hera/tests/mocksqlsrv/dummy"
     "strconv"
)

/*
* Creates a new server and opens up for connections.
*/
func main() {

     if len(os.Args) < 3 {
          log.Fatal("usage: ./runserver port frac\n\tport is a port number on localhost\n\tfrac is percentage failure")
     }
     port := os.Args[1]
     frac := os.Args[2]

     cflags := uint32(0) | uint32(dummy.CLIENT_PROTOCOL_41)
     s, err := dummy.CreateServer(cflags, "testserver.06.19", port)
     if err != nil {
          log.Fatal(err)
     }
     defer s.Close()

     // Accept any incoming connections. (Assumes that the client will
     // disconnect themselves. Does not disconnect by itself.
     for {
          cnxn, err := s.Accept()
          if err != nil {
               log.Fatal(err)
          }
          f, err := strconv.ParseFloat(frac, 32)
          if err != nil {
               log.Fatal(err)
          }
          conn := s.NewConnection(cnxn, f)

          // Exchange capabilities and authentication data. Will pass
          // control to the client in the command phase if successful.
          go s.Handle(conn)
     }
}
