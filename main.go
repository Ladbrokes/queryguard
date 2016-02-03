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
	"flag"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	messageTimeout := flag.Duration("message_timeout", 2*time.Minute, "timeout for one message to be proxied")
	clientIdleTimeout := flag.Duration("client_idle_timeout", 60*time.Minute, "idle timeout for client connections")
	addrs := flag.String("addrs", "localhost:27017", "comma separated list of mongo addresses")
	listen := flag.String("listen", ":6000", "port to listen for connections on")
	user := flag.String("username", os.Getenv("GUARD_USER"), "backchannel authentication - ENV{GUARD_USER}")
	pass := flag.String("password", os.Getenv("GUARD_PASS"), "backchannel authentication - ENV{GUARD_PASS}")
	authdb := flag.String("authenticationDatabase", os.Getenv("GUARD_AUTHDB"), "backchannel authentication - ENV{GUARD_AUTHDB}")

	flag.Parse()

	servers := []string{}
	for _, server := range strings.Split(*addrs, ",") {
		servers = append(servers, strings.TrimSpace(server))
	}

	log.Println("Listening on", *listen)
	log.Println("Forwarding to", strings.Join(servers, ", "))

	if err := newProxy(servers, *user, *pass, *authdb, *messageTimeout, *clientIdleTimeout).ListenAndRelay("tcp", *listen); err != nil {
		panic(err)
	}
}
