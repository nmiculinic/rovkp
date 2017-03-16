./sbin/start-dfs.sh
hadoop fs -ls /user/rovkp
wget http://svn.tel.fer.hr/gutenberg.zip
hadoop fs -put gutenberg.zip /user/rovkp
./bin/hdfs fsck /user/rovkp/gutenberg.zip -files -blocks -locations

Status: HEALTHY
 Total size:	158790992 B
 Total dirs:	0
 Total files:	1
 Total symlinks:		0
 Total blocks (validated):	2 (avg. block size 79395496 B)
 Minimally replicated blocks:	2 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	1
 Average block replication:	1.0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
 Number of data-nodes:		1
 Number of racks:		1
FSCK ended at Thu Mar 16 11:06:53 CET 2017 in 0 milliseconds
The filesystem under path '/user/rovkp/gutenberg.zip' is HEALTHY

2 bloka, replikacijski faktor 1. Relativno je mala datoteka, moze se cijela cachat u memoriju. HDFS je za vece datoteke

hadoop fs -get /user/rovkp/gutenberg.zip  /tmp
md5sum gutenberg.zip 
e3fc0eb2c51e0290c9b85fd2a7cee071  gutenberg.zip
md5sum /tmp/gutenberg.zip 
e3fc0eb2c51e0290c9b85fd2a7cee071  /tmp/gutenberg.zip

