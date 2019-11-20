# massive_geoip_join_spark

一般地，GeoIP 通过CIDR或者段起始-段结束的方式去记录数据。</br>
那么， 常规的GeoIP匹配，就是在所有记录中，查找IP地址所在的IP段，读取这条记录中的其他信息。


```
int
rangelist_is_contains(const struct RangeList *targets, unsigned addr)
{
    unsigned i;
    for (i=0; i<targets->count; i++) {
        struct Range *range = &targets->list[i];

        if (range->begin <= addr && addr <= range->end)
            return 1;
    }
    return 0;
}
```
上面是masscan中使用的函数，用于查找一个IP所属的IP段。</br>
其原理是遍历所有记录，找到符合 range->begin <= addr && addr <= range->end 条件的记录。

在实时的业务处理逻辑中，一般都使用此类方式进行。但在Spark中处理大批量数据时，不能在通过这种方式进行。

假设：
有一批5000万规模的IP地址样本，需要进行归属地匹配。</br>
如果使用比较段起始段结束的方式去匹配，需要匹配百亿次才能得出结果。(IPIP.net提供的城市级精度IP归属地数据共有约350万行记录)
使用我的这种方式，可以在有限的时间内完成匹配。


## 概念
请先知晓以下两个概念：</br>
1、IP地址可以转换成整数形式，严谨地说，应该是unsigned int，但long比较方便，不容易出错。</br>
2、一份好的GeoIP数据应当完全覆盖IPv4空间，每一条记录表示从ipStart到ipEnd中间所有的IP地址。每两条记录之间，连续，且互不重叠。

##原理

为了便于解读，下面表格中的IP用字符串形式做讲解

select ipStart,ipEnd,country,province,city,'0' as flag from where.your_geoip_stored order by ipStart asc

| ipStart | ipEnd | country | province | city | flag |
|---------|-------|---------|----------|------|------|
| ...| ... |  ...  |  ...  |  ... |  0 |
| 117.61.0.0| 117.61.255.255 |   中国  |   江苏   |   南京  | 0 |
| 117.71.0.0 | 117.71.15.255 |   中国  |   安徽   |   合肥  | 0 |
| ...| ... |  ...  |  ...  |  ... | 0 |


select ip as ipStart,unll as ipEnd,null as country,null as province,null as city,'1' as flag from your.sample

| ipStart | ipEnd | country | province | city | flag |
|---------|-------|---------|----------|------|------|
| 171.61.31.0| null |  null  |  null  |  null |  1 |
| 171.61.31.212 | null |  null  |  null  |  null |  1 |
| 171.71.14.2| null |  null  |  null  |  null |  1 |

上面两个表或者Dataframe跨度一致，做UNION, 并按照ipStart排序


| ipStart | ipEnd | country | province | city | flag |
|---------|-------|---------|----------|------|------|
| ...| ... |  ...  |  ...  |  ... |  0 |
| 117.61.0.0| 117.61.255.255 |   中国  |   江苏   |   南京  | 0 |
| 171.61.31.0| null |  null  |  null  |  null |  1 |
| 171.61.31.212 | null |  null  |  null  |  null |  1 |
| 117.71.0.0 | 117.71.15.255 |   中国  |   安徽   |   合肥  | 0 |
| 171.71.14.2| null |  null  |  null  |  null |  1 |
| ...| ... |  ...  |  ...  |  ... | 0 |

这样，IP段数据与样本数据全部合并成一张表，仅靠flag字段做区分。</br>
每一个待匹配的IP，紧跟着所属的IP段后面,只需要按照特定的当时填充null值即可.

```
w = Window.orderBy("ipSpart")
```

创建一个窗口

```
for colname in uTable.columns[1:-1]:
	uTable = uTable.withColumn(
			colname, 
			F.when(
				F.col(colname).isNull(), 
				F.last(F.col(colname),True).over(w)
		).otherwise(F.col(colname)))
```

对每一列中的null进行赋值。

	F.last(F.col(colname),True).over(w)

表示从窗口中读取，截止到当前行，最后一个非空数值。

| ipStart | ipEnd | country | province | city | flag |
|---------|-------|---------|----------|------|------|
| ...| ... |  ...  |  ...  |  ... |  0 |
| 117.61.0.0| 117.61.255.255 |   中国  |   江苏   |   南京  | 0 |
| 171.61.31.0| 117.61.255.255 |   中国  |   江苏   |   南京  |  1 |
| 171.61.31.212 | 117.61.255.255 |   中国  |   江苏   |   南京  |  1 |
| 117.71.0.0 | 117.71.15.255 |   中国  |   安徽   |   合肥  | 0 |
| 171.71.14.2| 117.71.15.255 |   中国  |   安徽   |   合肥  |  1 |
| ...| ... |  ...  |  ...  |  ... | 0 |


最后根据flag字段过滤出样本记录，对列做适当加工接即可。

| ip | country | province | city |
|---------|---------|----------|------|
| 171.61.31.0 |   中国  |   江苏   |   南京  |
| 171.61.31.212 |   中国  |   江苏   |   南京  |
| 171.71.14.2|  中国  |   安徽   |   合肥  |


实际测试中：

IP地址数量：5000万</br>
GeoIP记录数：380万</br>
Driver Memory：9G</br>
Executor: 4</br>
Time: 27s</br>
