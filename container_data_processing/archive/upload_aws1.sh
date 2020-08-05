#!/bin/bash

cities=('Minneapolis' 'LosAngeles' 'Denver' 'Miami' 'Memphis' 'NewYork' 
		   'Louisville' 'Columbus' 'Atlanta' 'Washington' 
	       'Chicago' 'Boston' 'Oakland' 'StLouis' 'Portland' 
	       'Seattle' 'Houston' 'SanFrancisco' 'Philadelphia' 'Baltimore')

cd 
cd  big_data_training/python/dash_demo1/data_cumulative/city_date/

for i in ${!cities[@]};
do 
	city=${cities[$i]}
	printf "%s\n" "$city"
	aws s3 cp $city/stats/ s3://kotasstorage1/app_data/data_cumulative/city_date/$city/stats --recursive
done
