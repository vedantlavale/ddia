# There are mainly three algorithms used in distributing the load in load balancers

- Round Robin — requests cycle through servers in order (s1 → s2 → s3 → s1...). Simple and fair when all servers are roughly equal in power. This is the default for most setups.

- Least Connections — always picks the server with the fewest active connections. Much smarter when some requests are slow (e.g., a DB query that takes 2 seconds). Without this, a slow server can pile up connections while others sit idle.

- IP Hash — the same client IP always routes to the same server. This is called a "sticky session." You need it when session state is stored in memory on the server (e.g., user login state in a variable), not in a shared Redis/DB. The downside: if that server goes down, that user's session is lost.
