package dummy

/*
* Simple server implementation that handles connection phase of MySQL protocol.
* Contains a Server struct as well as functions for creating a server,
* accepting connections, handling connections, closing the listener (which
* listens for connections) and creating a Connection object (which includes the
* actual connection object and more fields).
*/

/*== IMPORTS =================================================================*/
import (
     "net"
)

/*== FUNCTIONS ===============================================================*/

// Server struct stores server capabilities, a listener for receiving
// connections, the number of connections, and a handler that
// manages connections.
type Server struct {
     capabilities   uint32        // Server configuration bitmask
     listener       net.Listener  // Server socket for receiving connections
     handler        Handler       // Handles connections from the listener
     connections    int           // Number of connections to the server
     server_ver     string        // server version
}

/*
* Creates a listener on a local port. Arguments can be changed
* by replacing prot and DSN for desired protocol and port.
*/
func CreateListener(port string) (net.Listener, error){
     prot := "tcp"
     DSN := "localhost:" + port
     listener, err := net.Listen(prot, DSN)
     if err != nil {
          return nil, err
     }
     return listener, nil
}

/*
* Initializes a server with a listener, handler, the input configuration (which
* is a bit mask detailing the capabilities of the server), and
* initializes the number of connections to 0.
*/
func CreateServer(config uint32, ver string, port string) (Server, error) {

     // Initialize listener for the server.
     l, err := CreateListener(port)
     if err != nil {
          return Server{}, err
     }

     // Initialize handler for the server.
     h := CreateHandler()

     return Server{capabilities:config, listener: l, handler: h,
                         connections:0, server_ver:ver}, nil
}

/*
* Calls the handle() function for the server's handler. This requires
* passing in the server's capabilities. Is abstracted one layer above
* for cleaner code.
*/
func (s *Server) Handle(cnxn *Conn) {
     s.handler.handle(cnxn, s.capabilities)
}

/*
* Calls the Close() function for the server's listener.
*/
func (s *Server) Close() {
     s.listener.Close()
}

/*
* Calls the Accept() function for the server's listener.
*/
func (s *Server) Accept() (net.Conn, error){
     return s.listener.Accept()
}

/*
* Calls CreateConnection which sets the connection's capabilities
* according to the server's initial configuration. The capabilities
* are later set when the handshake response is received. The connection id
* is the number of connections (which increments with each new connection).
*/
func (s *Server) NewConnection(c net.Conn, frac float64) *Conn {
     conn := CreateConnection(c, s.capabilities, s.connections, s.server_ver, frac)
     s.connections++
     return conn
}
