#!/bin/bash

sudo java -cp 'host/target/confidentialstorm-topology.jar:/opt/storm/lib/*' ch.usi.inf.confidentialstorm.WordCountTopology --local
