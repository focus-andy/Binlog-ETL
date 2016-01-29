# Binlog-ETL
Binlog-ETL is a system designed for synchronising the data in MySQL to HIVE data warehouse on Hadoop. The best feature is that it can keep
the latest snapshot of MySQL tables in the DW while MySQL databases are updating.<br /><br />

<h3>Binlog</h3>
Binlog is the short for binary log. The binary log of MySQL contains “events” that describe database changes such as table creation 
operations or changes to table data. It is also used for MySQL Master-Slave synchronization. So in this repository, it is designed to 
be used for synchronising MySQL tables to Data Wharehouse constructed with HIVE and Hadoop.<br /><br />

#Features
<ul>
<li>Create HIVE tables and partitions</li>
<li>Parse MySQL binary logs and extract updates</li>
<li>Upload data to HDFS</li>
<li>Support multi-processes</li>
<li>Support MySQL sharding databases and tables</li>
<li>Keep the latest snapshot and remove duplicates</li>
<li>The above features are all automatical</li>
</ul>

#Requriements
<ul>
<li>MySQL version 5.6 or newer</li>
<li>Enable binary log in my.cnf</li>
</ul>

#Data Flow
The Binlog-ETL system lays between MySQL cluster and HIVE data warehouse. It requests bin-logs from MySQL cluster initiativly and 
creates snapshots automatically.
<ul>
<li>Step 1, request latest bin-logs and download them to local disks</li>
<li>Step 2, parse bin-logs and transform the data to target format</li>
<li>Step 3, upload the new data to HDFS. The data are the latest updates of MySQL tables</li>
<li>Step 4, merge the new data with the old snapshot, keep the latest updates and remove the duplicates.</li>
<li>Step 5, Create new a snapshot and end.</li>
</ul>

