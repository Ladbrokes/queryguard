# queryguard
`queryguard` is a simple 1:1 proxy for mongodb that prevents people from running queries that won't use indexes

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
iptables -t nat -A queryguard -s 10.36.1.49/32 -j ACCEPT
iptables -t nat -A queryguard -s 10.36.1.8/32 -j ACCEPT
iptables -t nat -A queryguard -s 10.36.5.8/32 -j ACCEPT

iptables -t nat -A PREROUTING -p tcp -m tcp --dport 27017 -j queryguard
iptables -t nat -A PREROUTING -p tcp -m tcp --dport 27017 -j REDIRECT --to-ports 29017
```
