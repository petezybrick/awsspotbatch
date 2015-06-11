#!/bin/bash
if [ -z "$1" ] 
	then set $1 5
fi

echo "Before sleeping for" $1 "secs"
sleep $1
echo "After sleep"
exit 123
