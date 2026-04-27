sed -i -e '/pingTicker := time.NewTicker(getMultiplexedPingPeriod())/i \
pingDone := make(chan struct{})\
defer close(pingDone)\
\
go func() {\
gTicker := time.NewTicker(getMultiplexedPingPeriod())\
gTicker.Stop()\
gDone:\
\
gTicker.C:\
n := mwc.conn\
lock()\
n == nil {\
tinue\
gSendCounter := atomic.AddInt64(\&mwc.pingSendCount, 1)\
n.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(getMultiplexedWriteWait())); err != nil {\
gSendCounter == 1 || pingSendCounter%10000 == 0 {\
tf("⚠️ Ошибка отправки ping: %v (всего попыток: %d)", err, pingSendCounter)\
ore error, read loop will catch connection failures\
fig.Timeframe)\
n.SetReadDeadline(time.Now().Add(readTimeout))\
t64(\&mwc.pingCount, 1)\
gTime = time.Now()\
lock()\
ance/multiplexed_ws_client.go
