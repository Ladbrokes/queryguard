# queryguard
`queryguard` is a simple 1:1 proxy for mongodb that prevents people from running queries that won't use indexes

We've run into an issue with a 260gb+ dataset where people were accidently querying it with fields that either didn't even exist or were unindexed, `queryguard` lets
us resolve both of these issues at the same time by only permitting queries that will be using a query - admittedly this is done naively but with the hope the edge
cases result in failed queries not absurdly high mongodb load.

## Features

 * Completely transparant, sits right in between your regular clients and mongo
 * Can authenticate against the mongo using different credentials than the clients are using

## Building

	cd $GOPATH
	go get github.com/Ladbrokes/zookeeper

## Running it

### With runnit

```
#!/bin/bash
# Usage of ./queryguard_linux_amd64:
#  -addrs string
#    	comma separated list of mongo addresses (default "localhost:27017")
#  -authenticationDatabase string
#    	backchannel authentication - ENV{GUARD_AUTHDB}
#  -client_idle_timeout duration
#    	idle timeout for client connections (default 1h0m0s)
#  -listen string
#    	port to listen for connections on (default ":6000")
#  -message_timeout duration
#    	timeout for one message to be proxied (default 2m0s)
#  -password string
#    	backchannel authentication - ENV{GUARD_PASS}
#  -username string
#    	backchannel authentication - ENV{GUARD_USER}

exec 2>&1
echo "Service was (re)started"
export GUARD_PASS=`cat /data/logsearch/.auth-root`
exec ./queryguard_linux_amd64 -username root -authenticationDatabase admin -listen ":29017"
```

### With iptables redirect

```
#!/bin/bash

iptables -t nat -N queryguard

# Whitelisted addresses - anything from these addresess won't go to queryguard
iptables -t nat -A queryguard -s 10.1.2.3/32 -j ACCEPT
iptables -t nat -A queryguard -s 10.1.2.4/32 -j ACCEPT
iptables -t nat -A queryguard -s 10.1.2.5/32 -j ACCEPT
iptables -t nat -A queryguard -s 10.1.2.6/32 -j ACCEPT
iptables -t nat -A queryguard -j RETURN

iptables -t nat -A PREROUTING -p tcp -m tcp --dport 27017 -j queryguard
iptables -t nat -A PREROUTING -p tcp -m tcp --dport 27017 -j REDIRECT --to-ports 29017
```

## Credit

Inspiration, and protocol.go sourced from the [Dvara](https://github.com/facebookgo/dvara) project

## License

Copyright (c) 2015 Shannon Wynter, Ladbrokes Digital Australia Pty Ltd. Licensed under GPL3. See the [LICENSE.md](LICENSE.md) file for a copy of the license.
