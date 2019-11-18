from pyspark.sql.functions import udf
from pyspark.sql.types import LongType,StringType
from pyspark.sql import functions as F
from pyspark.sql import Window

ip4_to_long = lambda ip: sum([256 ** j * int(i) for j, i in enumerate(ip.split('.')[::-1])])
long_to_ip4 = lambda ip: '.'.join([str(int(ip/(256 ** i)) % 256) for i in range(3, -1, -1)])
# ipaddress convert bewteen string and long int

ip2long = udf(ip4_to_long, LongType())
long2ip = udf(long_to_ip4, StringType())
# registe as udf, u will need it.


def main(sparkSession):

    sql = """
      select 
        ipStart, -- long type
        ipEnd, -- long type
        country,
        province,
        city,
        isp,
        '0' as flag
      from 
        where.you_geo_info_stored """

    GeoData = sparkSession.sql(sql)

    sql = """
        select 
          ip as ipStart, -- long type
          null as ipEnd,
          null as country,
          null as province,
          null as city,
          null as isp,
          '1' as flag
        from
          data_sample
        where
          other condition
    """

    sample = sparkSession.sql(sql)

    uTable = GeoData.union(sample).orderBy("ipStart", Ascending=True)

    w = Window.orderBy("ipStart")

    for colname in uTable.columns[1:-1]:
        uTable = uTable.withColumn(colname, F.when(F.col(colname).isNull(), F.last(F.col(colname),True).over(w)).otherwise(F.col(colname)))


    res = uTable.filter(uTable.flag=='1').withColumnRenamed("ipStart","ip")[["ip","country","province","city","isp"]]
