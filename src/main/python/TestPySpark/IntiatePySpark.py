
from pyspark import SparkContext, SparkConf
from hdfs import InsecureClient
import os
#import org.apache.spark.util.SizeEstimator

#conf = (SparkConf().setMaster('local').setAppName('a'))
#print(sc.textFile("C:\\Users\\User\\Desktop\\data1.txt").first())


#client_hdfs = InsecureClient()

# os.environ["SPARK_HOME"] = "C:\\spark-1.6.3-bin-hadoop2.6"
#     # print(.environ['PATH'])
# sc = SparkContext(master="local", appName="Spark Demo")

class Intitalization:

    def sparkContextIntialize(self):
        os.environ["SPARK_HOME"] = "C:\\spark-1.6.3-bin-hadoop2.6"
        # print(.environ['PATH'])
        sc = SparkContext(master="local", appName="Spark Demo")
        return sc

I = Intitalization()
sc = I.sparkContextIntialize()
class RowLevleTransform:

     def string_manupulation(self):

         orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
         s = orders.first()

         print(s)
         len(s)
         a = s[:10]
         b=int(s.split(",")[1].split(" ")[0].replace("-",""))
         print(b)

     def map_example(self):
         orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
         s1 = orders.map(lambda a : int(a.split(",")[1].split(" ")[0].replace("-",""))).first()
         print(s1)

     def flatmap_ex(self):
         linesList = ["How are you", "let us perform", "word count using flatMap", "to understand flatMap in detail"]
         lines = sc.parallelize(linesList)
         for i in lines.collect() : print(i)
         words = lines.flatMap(lambda  a: a.split(" "))
         tuples = words.map(lambda  b: (b,1))
         for i in words.take(10) : print(i)
         print("------------------------------------------")
         #for i in tuples.countByKey(): print(i)

     def filter_ex(self):
         orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
         # test = orders.take(1)
         # print(test)
         orderscomplete = orders.\
             filter(lambda a : a.split(",")[3] in ("CLOSED", "COMPLETE") and a.split(",")[1][:7]  == "2014-01" )
         #a = orderscomplete.count
         for i in orderscomplete.take(10): print(i)

class Joins:

     def innerjoin_ex(self):
         orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         # for i in orders.take(10): print(i)
         # for j in orderitems.take(10) : print(j)
         ordersmap = orders.map(lambda a : (int(a.split(","))[0], a.split(",")[1]))
         orderitemsmap = orderitems.map(lambda a : (int(a.split(","))[2], float(a.split(",")[4])))

         joined = ordersmap.join(orderitemsmap)
         for i in joined.take(10) : print(i)

 # AGGRIGATION TOTAL REVENUE
     def aggregation_revenue(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         print(orderitems.count())
         ordersfiltered = orderitems.filter(lambda a: int(a.split(",")[1]) == 2)
         #for i in ordersfiltered.take(3): print(i)
         orderitemssubtotal = ordersfiltered.map(lambda b: float(b.split(",")[4]))
         from operator import add
         #print(orderitemssubtotal.reduce(add))
         print(orderitemssubtotal.reduce(lambda x,y : x+y))

     def aggregation_reduce(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         filtered = orderitems.filter(lambda a : int(a.split(",")[1])==2)
         return print(filtered.reduce(lambda x,y: x if float(x.split(",")[4]) < float(y.split(",")[4]) else y ))

     def countbykey_ex(self):
         orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
         #for i in orders.take(10): print(i)
         orderstatus_filter = orders.map(lambda a: (a.split(",")[3],1))
         orderstatus = orderstatus_filter.countByKey()
         return print(orderstatus)

     def groupby_ex(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         orderItemsMap = orderitems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
         orderitemsgrouped = orderItemsMap.groupByKey()
         #for i in orderitemsgrouped.take(10): print(i)
         orderitemsrevenure = orderitemsgrouped.map(lambda x : (x[0], round(sum(x[1]), 2)))
         for i in orderitemsrevenure.take(10) : print(i)

     # Get order item detail in decesnding order using groupBykey
     def groupby_ex1(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         #for i in orderitems.take(10) : print(i)
         orderItemsMap = orderitems.map(lambda oi: (int(oi.split(",")[1]), oi))
         orderitemsgrouped = orderItemsMap.groupByKey()
         orderitemssorted = orderitemsgrouped.flatMap(lambda op : sorted(op[1], key= lambda a : float(a.split(",")[4]), reverse=True))
         for i in orderitemssorted.take(10) : print(i)

     # Get revenue for each order_id - reduceByKey
     def reducebykey_ex(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         revenue = orderitems.map(lambda oi : (int(oi.split(",")[2]), float(oi.split(",")[4]))).\
              reduceByKey(lambda a, b : round(a+b,2))
         from operator import add
         revenue_usingadd = orderitems.map(lambda oi : (int(oi.split(",")[2]), float(oi.split(",")[4]))). \
             reduceByKey(add)
         for i in revenue.take(10) : print(i)

     #Get order item id with minimum revenue for each order_id - reduceByKey
     def reducebykey_minrevenue(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         minrevenue = orderitems.map(lambda oi : (int(oi.split(",")[2]), float(oi.split(",")[4]))). \
                      reduceByKey(lambda a, b : a if a < b else b)
         for i in minrevenue.take(10) : print(i)

     #Get order item details with minimum subtotal for each order_id - reduceByKey
     def reducebykey_minorderdetail(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         minorderdetails = orderitems.map(lambda x : (int(x.split(",")[2]), x))
         minrevenue = minorderdetails.\
             reduceByKey(lambda a, b : a if (float(a.split(",")[4]) < float(b.split(",")[4])) else b)
         for i in minrevenue.take(10) : print(i)

     #Get revenue and count of items for each order id - aggregateByKey
     def aggregartebykey_ex(self):
         orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         orderitemmap = orderitems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
         revandcount = orderitemmap.aggregateByKey((0.0,0),
                       lambda x,y: (x[0]+y,x[1]+1),
                       lambda x,y : (x[0]+y[0], x[1]+y[1]) )
         for i in revandcount.take(10) : print(i)

     #Sort data by product price - sortByKey
     def simplesort_ex(self):
         products = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
         #for i in products.take(10) : print(i)
         sortorder = products.filter(lambda a : a.split(",")[4] != ""). \
                     map(lambda p: (float(p.split(",")[4]), p))
         productsSortedByPrice = sortorder.sortByKey().map(lambda op : op[1])
         for i in productsSortedByPrice.take(10) : print(i)

     # Composit Sort -> key will have a pair
     # take ordered or top -> top will give in decending order and takeordered will sort in ascending order
     def topnrank_ex(self):
        products = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\products")
        productsMap = products.filter(lambda x: x.split(",")[4] != "").map(lambda y : (int(y.split(",")[1]),y))
        productsGroupby = productsMap.groupByKey()
        #for i in productsGroupby.take(10) : print(i)
        t = productsGroupby.first()
        l = sorted(t[1], key=lambda a : float(a.split(",")[4]), reverse=True )
        l[:3]
        topnproductsbycategory = productsGroupby. \
            flatMap(lambda op : sorted(op[1], key= lambda x : float(x.split(",")[4]),reverse=True)[:3])
        for i in topnproductsbycategory.take(10) : print(i)
#
#
#



o1 = RowLevleTransform()
j = Joins()

#o1.string_manupulation()
#o1.map_example()
#o1.flatmap_ex()
#o1.filter_ex()

#j.innerjoin_ex()
#j.aggregation_revenue()
#j.aggregation_reduce()
#j.countbykey_ex()
#j.groupby_ex()
#j.groupby_ex1()
#j.reducebykey_ex()
#j.reducebykey_minrevenue()
#j.reducebykey_minorderdetail()
#j.aggregartebykey_ex()
#j.simplesort_ex()

#j.topnrank_ex()
