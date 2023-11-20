# BGP Zombies / BGP Stuck routes

### What are BGP Zombies?
A BGP zombie refers to an active routing table entry for a prefix that has been withdrawn by its origin network, and is hence not reachable anymore. Hereafter we also refer to zombie ASes and zombie peers for ASes and BGP peers whose routers have BGP zombies.

RIPE Labs refers to the issue `When withdrawing an IP prefix from the Internet, an origin network sends BGP withdraw messages, which are expected to propagate to all BGP routers that hold an entry for that IP prefix in their routing table. Yet network operators occasionally report issues where routers maintain routes to IP prefixes withdrawn by their origin network.` as stuck routes / ghost routes / BGP Zombies. 

This repository is a parser which analyses the BGP updates and BGP dumps for a time period to identify any stuck routes. 

### How to run?

`usage: run.py [-h] -s STARTDATE -e ENDDATE -r RRC {intervals,zombies}`

STARTDATE = From when do you wanna identify stuck routes
ENDDATE = To when do you wanna identify stuck routes
RRC = Route collectors, which are physical machines where RIS ingests BGP routing data.

### Example:  

`python3 run.py -s 20170909 -e 20170915 -r rrc06 intervals`
To download the BGP updates files and create intervals. 

`python3 run.py -s 20170909 -e 20170915 -r rrc06 zombies`
To find the BGP stuck routes in that interval
