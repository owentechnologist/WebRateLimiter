#!/bin/sh
# this script expects an argument that determines how many
# # iterations to perform for each accountID: (NB: 300 iterations should work well)
# this script also expects an argument that determines how many
# # variations for accountID to use (NB: smaller # variants causes rate limiting to kick in)
# example execution:  > /littlewrlloader.sh 300 25
iterations=$1
variants=$2
for ((i =0; i < iterations; i++)) do
    http get :4567/cleaned-submissions User-Agent:Mozilla/5.0 accountKey==0$(( i % variants))
    http get :4567/top10-submissions User-Agent:Mozilla/5.0 accountKey==0$(( i % variants/2))
    if [ $((iterations%2))==0 ]
    then
      http :4567/correct-city-spelling accountKey==00$(( i % variants)) uniqueRequestKey==PM_UID539526 city==tauranto
    else
      http :4567/correct-city-spelling accountKey==00$(( i % variants)) uniqueRequestKey==PM_UID539526 city==broowklean
    fi
    sleep 1

done