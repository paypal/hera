package dummy

import "fmt"

type Handler struct {
}

// As of right now, just returns a simple Handler.
func CreateHandler() (Handler) {
     return Handler{}
}

/* Begin connection phase communication between client and server.
* In current testing phase, command phase is a simple 'test command'
* which returns a garbage response.
*/
func (h *Handler) handle(c *Conn, config uint32) {

     // Set initial handshake packet.
     c.sendHandshake(HANDSHAKEv10)
     // fmt.Println(c.GetSequenceID())

     // Read handshake response packet from client.
     c.readPacket(true)
     // fmt.Println(c.GetSequenceID())

     // Send OK packet.
     c.sendOKPacket("Welcome!")

     // Switch over to command phase to receive commands from the client.
     // receiveCommand is what 'handles' the commands, so nothing else
     // needs to be done out here.
     for {
          if !c.receiveCommand() { break }
     }
     fmt.Println("Closing connection.")
     c.CloseConnection()
}
