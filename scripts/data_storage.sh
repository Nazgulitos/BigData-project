# echo "Building Postgres database is starting!"
# python scripts/build_projectdb.py
# echo "Building Postgres database is done!"

echo "Copying data into HDFS starts.."
password=$(head -n 1 secrets/.psql.pass)

sqoop list-databases --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password
sqoop list-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team15_projectdb --username team15 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1
echo "Copying data into HDFS is done!"