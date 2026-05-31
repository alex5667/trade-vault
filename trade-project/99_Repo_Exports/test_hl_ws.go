package main

import (
    "log"
    "net/http"
    "time"
    "net"
    "context"

    "github.com/gorilla/websocket"
)

func main() {
    dialer := websocket.Dialer{
        Proxy: http.ProxyFromEnvironment,
        HandshakeTimeout: 10 * time.Second,
        NetDial: func(network, addr string) (net.Conn, error) {
            nd := &net.Dialer{
                Timeout:   10 * time.Second,
                KeepAlive: 60 * time.Second,
            }
            return nd.DialContext(context.Background(), "tcp4", addr)
        },
    }
    log.Println("Dialing...")
    conn, res, err := dialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
    if err != nil {
        log.Fatalf("Dial err: %v", err)
    }
    log.Printf("Connected: status=%d", res.StatusCode)

    cmd := []byte(`{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}`)
    if err := conn.WriteMessage(websocket.TextMessage, cmd); err != nil {
        log.Fatalf("Write err: %v", err)
    }

    log.Println("Reading...")
    for i := 0; i < 5; i++ {
        conn.SetReadDeadline(time.Now().Add(5*time.Second))
        _, msg, err := conn.ReadMessage()
        if err != nil {
            log.Fatalf("Read err: %v", err)
        }
        log.Printf("Msg: %s", string(msg))
    }
    conn.Close()
}
