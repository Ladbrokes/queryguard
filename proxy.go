package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/davecgh/go-spew/spew"
)

const headerLen = 16

var (
	cmdCollectionSuffix  = []byte(".$cmd\000")
	errNormalClose       = errors.New("normal close")
	errClientReadTimeout = errors.New("client read timeout")
	errorBytes           = []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
)

type Proxy struct {
	servers     []string
	backChannel *mgo.Session

	clientIdleTimeout time.Duration
	messageTimeout    time.Duration
}

func (p *Proxy) newServerConn() (net.Conn, error) {
	lastServer := ""
	retrySleep := 50 * time.Millisecond
	for retryCount := 7; retryCount > 0; retryCount-- {
		server := p.servers[0]
		if l := len(p.servers); l > 1 {
			server = p.servers[rand.Intn(l-1)]
		}
		c, err := net.Dial("tcp", server)
		if err == nil {
			return c, nil
		}
		log.Println("Unable to connect to", server, ":", err, "retrying in", retrySleep/time.Microsecond)
		time.Sleep(retrySleep)
		retrySleep = retrySleep * 2
		lastServer = server
	}
	return nil, fmt.Errorf("Couldn't connect to %s", lastServer)
}

func (p *Proxy) handleClientConnection(c net.Conn) {
	s, err := p.newServerConn()
	if err != nil {
		log.Println("Server failure", err)
		c.Close()
		return
	}

	defer func() {
		s.Close()
	}()

	if conn, ok := c.(*net.TCPConn); ok {
		conn.SetKeepAlivePeriod(2 * time.Minute)
		conn.SetKeepAlive(true)
	}

	for {
		h, err := p.clientReadHeader(c)
		if err != nil {
			if err != errNormalClose {
				log.Println(err)
			}
			return
		}
		err = p.handleMessage(h, c, s)
		if err != nil {
			log.Println(err)
		}
	}
}

func (p *Proxy) clientReadHeader(c net.Conn) (*messageHeader, error) {
	c.SetReadDeadline(time.Now().Add(p.clientIdleTimeout))

	header, err := readHeader(c)
	if err == nil {
		return header, nil
	}

	if err == io.EOF {
		return nil, errNormalClose
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, errClientReadTimeout
	}

	return nil, err
}

func (p *Proxy) handleMessage(h *messageHeader, client, server net.Conn) error {
	deadline := time.Now().Add(p.messageTimeout)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)

	if h.OpCode == OpQuery {
		p.handleQueryRequest(h, client, server)
		return nil
	}

	if err := h.WriteTo(server); err != nil {
		log.Println(err)
		return err
	}

	if _, err := io.CopyN(server, client, int64(h.MessageLength-headerLen)); err != nil {
		log.Println(err)
		return err
	}

	if h.OpCode.HasResponse() {
		if err := copyMessage(client, server); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (p *Proxy) handleQueryRequest(h *messageHeader, client, server io.ReadWriter) error {
	parts := [][]byte{h.ToWire()}

	var flags [4]byte
	if _, err := io.ReadFull(client, flags[:]); err != nil {
		log.Println(err)
		return err
	}
	parts = append(parts, flags[:])

	fullCollectionName, err := readCString(client)
	if err != nil {
		log.Println(err)
		return err
	}
	fullCollectionString := string(fullCollectionName[:len(fullCollectionName)-1])

	parts = append(parts, fullCollectionName)
	var twoInt32 [8]byte
	if _, err := io.ReadFull(client, twoInt32[:]); err != nil {
		log.Println(err)
		return err
	}
	parts = append(parts, twoInt32[:])

	queryDoc, err := readDocument(client)
	if err != nil {
		log.Println(err)
		return err
	}
	parts = append(parts, queryDoc)
	var q bson.D
	if err := bson.Unmarshal(queryDoc, &q); err != nil {
		log.Println(err)
		return err
	}

	log.Printf("Buffered OpQuery for %s: %s", fullCollectionString, spew.Sdump(q))

	if !bytes.HasSuffix(fullCollectionName, cmdCollectionSuffix) && len(q) > 0 {
		database, collection := p.splitDatabaseCollection(fullCollectionString)
		if !p.checkForIndex(database, collection, q) {
			return p.sendErrorToClient(h, client, fmt.Errorf("No index was found that could be used for your query try db.%s.getIndexes()", collection))
		}
	}

	var written int
	for _, b := range parts {
		n, err := server.Write(b)
		if err != nil {
			log.Println(err)
			return err
		}
		written += n
	}

	pending := int64(h.MessageLength) - int64(written)
	if _, err := io.CopyN(server, client, pending); err != nil {
		log.Println(err)
		return err
	}

	if err := copyMessage(client, server); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (p *Proxy) checkForIndex(databaseName, collectionName string, query bson.D) bool {
	c := p.backChannel.Clone().DB(databaseName).C(collectionName)

	if q := p.convertQuery(c, query); q != nil {
		m := struct {
			IndexBounds []interface{} `bson:"indexBounds"`
		}{}
		q.Explain(m)

		if m.IndexBounds == nil || len(m.IndexBounds) == 0 {
			return false
		}
	}

	// q is nil which means $explain existed, or IndexBounds > 0
	return true
}

func (p *Proxy) convertQuery(c *mgo.Collection, q bson.D) *mgo.Query {
	if q[0].Name == "query" && len(q) > 1 {
		if p.hasKey(q, "$explain") {
			return nil
		}
		query := c.Find(q[0].Value.(bson.D).Map())
		if p.hasKey(q, "orderby") {
			query.Sort(p.orderbytosort(p.getKey(q, "orderby")))
		}
		return query
	}
	return c.Find(q.Map())
}

func (p *Proxy) sendErrorToClient(h *messageHeader, client io.Writer, err error) error {
	errorDoc, _ := bson.Marshal(struct {
		Error string `bson:"$err"`
	}{
		Error: err.Error(),
	})
	errorHeader := &messageHeader{
		MessageLength: int32(headerLen + len(errorBytes) + len(errorDoc)),
		RequestID:     h.RequestID,
		ResponseTo:    h.RequestID,
		OpCode:        OpReply,
	}
	parts := [][]byte{errorHeader.ToWire(), errorBytes, errorDoc}
	for _, p := range parts {
		if _, err := client.Write(p); err != nil {
			return err
		}
	}
	return nil
}

func (p *Proxy) splitDatabaseCollection(fullName string) (string, string) {
	split := strings.SplitN(fullName, ".", 2)
	return split[0], split[1]
}

func (p *Proxy) hasKey(d bson.D, k string) bool {
	return p.getKey(d, k) != nil
}

func (p *Proxy) getKey(d bson.D, k string) bson.D {
	for _, v := range d {
		if strings.EqualFold(v.Name, k) {
			return v.Value.(bson.D)
		}
	}
	return nil
}

func (p *Proxy) orderbytosort(d bson.D) string {
	res := []string{}
	for _, v := range d {
		if v.Value.(float64) > 0 {
			res = append(res, v.Name)
		} else {
			res = append(res, "-"+v.Name)
		}
	}

	return strings.Join(res, ",")
}

func (p *Proxy) ListenAndRelay(proto, listen string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}

	p.backChannel, err = mgo.Dial(strings.Join(p.servers, ","))
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Client connection error:", err)
		}
		go p.handleClientConnection(conn)
	}
}

func newProxy(servers []string, messageTimeout, clientIdleTimeout time.Duration) *Proxy {
	return &Proxy{
		servers:           servers,
		messageTimeout:    messageTimeout,
		clientIdleTimeout: clientIdleTimeout,
	}
}
