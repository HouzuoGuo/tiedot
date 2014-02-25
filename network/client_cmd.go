/* tiedot command implementations - client side. */
package network

func (tc *Client) ShutdownServer() {
	tc.writeReq(SHUTDOWN)
	tc.ShutdownClient()
}
