/*
 *   Queryguard - Simple 1:1 proxy for mongodb that prevents people from running queries that won't use indexes
 *   Copyright (c) 2016 Shannon Wynter, Ladbrokes Digital Australia Pty Ltd.
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   Author: Shannon Wynter <http://fremnet.net/contact>
 */

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
	servers []string

	backChannel *mgo.Session
	user        string
	pass        string
	authdb      string

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
	remoteAddr := "unknown"
	if c, ok := client.(net.Conn); ok {
		remoteAddr = c.RemoteAddr().String()
	}

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

	if !bytes.HasSuffix(fullCollectionName, cmdCollectionSuffix) && len(q) > 0 {
		log.Printf("[%s] Checking OpQuery for %s: %s", remoteAddr, fullCollectionString, spew.Sdump(q))
		database, collection := p.splitDatabaseCollection(fullCollectionString)
		if !p.checkForIndex(database, collection, q) {
			log.Printf("[%s] Rejecting query", remoteAddr)
			// pinched the code value from https://github.com/mongodb/mongo/blob/master/docs/errors.md
			return p.sendErrorToClient(h, client, fmt.Errorf("No index was found that could be used for your query try db.%s.getIndexes()", collection), 17357)
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
	indexes, err := c.Indexes()
	if err != nil {
		fmt.Println(err)
	}
	indexFieldName := p.firstIndexableField(query)
	for _, index := range indexes {
		if strings.EqualFold(strings.Trim(index.Key[0], "-"), indexFieldName) {
			return true
		}
	}

	return false
}

func (p *Proxy) firstIndexableField(query bson.D) string {
	if strings.TrimLeft(query[0].Name, "$") == "query" && len(query) > 1 {
		if value, ok := query[0].Value.(bson.D); ok && len(value) > 0 {
			return value[0].Name
		}
		if orderby := p.getKey(query, "orderby"); orderby != nil {
			switch t := orderby.(type) {
			case bson.D:
				return t[0].Name
			case string:
				return strings.Trim(t, "-")
			default:
				log.Printf("Unknown type, %t", t)
			}
		}
	}
	return query[0].Name
}

func (p *Proxy) sendErrorToClient(h *messageHeader, client io.Writer, err error, code int) error {
	errorDoc, _ := bson.Marshal(struct {
		Error string `bson:"$err"`
		Code  int    `bson:"code"`
	}{
		Error: err.Error(),
		Code:  code,
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

func (p *Proxy) getKey(d bson.D, k string) interface{} {
	for _, v := range d {
		if strings.EqualFold(strings.TrimLeft(v.Name, "$"), k) {
			return v.Value
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

	connectionString := strings.Join(p.servers, ",")
	if p.user != "" && p.pass != "" {
		connectionString = fmt.Sprintf("mongodb://%s:%s@%s", p.user, p.pass, connectionString)
		if p.authdb != "" {
			connectionString = connectionString + "/?authSource=" + p.authdb
		}
	}
	p.backChannel, err = mgo.Dial(connectionString)
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

func newProxy(servers []string, user, pass, authdb string, messageTimeout, clientIdleTimeout time.Duration) *Proxy {
	return &Proxy{
		servers:           servers,
		messageTimeout:    messageTimeout,
		clientIdleTimeout: clientIdleTimeout,
		user:              user,
		pass:              pass,
		authdb:            authdb,
	}
}
