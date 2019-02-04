from TestPySpark.Test import Intitalization
from operator import add

sc = Intitalization().sparkContextIntialize()


class ProblemStatment:


    def PreviewData(self):

        orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
        orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
        products = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\products")
        categories = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\categories")
        departments = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\departments")
        customers = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\customers")
        for i in orders.take(10): print(i)
        for j in orderitems.take(10) : print(j)
        for i in products.take(10): print(i)
        for i in categories.take(10): print(i)
        for i in departments.take(10): print(i)
        for i in customers.take(10): print(i)

    # Filtering all completed and closed orders
    def FilterOrders(self):

        orders = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\orders")
        orderitems = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\order_items")
        filteredorders = orders.filter(lambda a : a.split(",")[3] in ("COMPLETE","CLOSED"))
        #for i in filteredorders.take(100): print(i)
        ordersmap = filteredorders.map(lambda a : (int(a.split(",")[0]), a.split(",")[1]))
        orderitemsmap = orderitems.map(lambda b : (int(b.split(",")[1]), (int(b.split(",")[2]),float(b.split(",")[4]))))
        orderjoin = ordersmap.join(orderitemsmap)
        #for i in orderjoin.take(100): print(i)
        initialrevenue = orderjoin.map(lambda op : ((op[1][0], op[1][1][0]), float(op[1][1][1]))).reduceByKey(lambda a,b : round(a+b,2))
        #for i in initialrevenue.take(10): print(i)
        #products = sc.textFile("C:\\Users\\User\\Desktop\\data\\retail_db\\products")
        productsRaw = open("C:\\Users\\User\\Desktop\\data\\retail_db\\products\\part-00000").read().splitlines()
        products = sc.parallelize(productsRaw)
        #for i in products.take(10) : print(i)
        productsmap = products.map(lambda op : (int(op.split(",")[0]),op.split(",")[2]))
        #for i in productsmap.take(10) : print(i)

        revenuepremap =  initialrevenue.map(lambda op : (op[0][1],(op[0][0],op[1]) ))
        #for i in revenuepremap.take(10) : print(i)

        resultnotsorted = revenuepremap.join(productsmap)
        #for i in resultnotsorted.take(10) : print(i)

        preresult = resultnotsorted.map(lambda op : ((op[1][0][0], -op[1][0][1]), (op[1][0][0], op[1][1],op[1][0][1])) )
        sorted = preresult.sortByKey()
        result = sorted.map(lambda x : x[1])
        for i in result.take(100) : print(i)

        # pre_result =  resultnotsorted.map(lambda x: x[1])
        # result = pre_result.map(lambda a : sorted(a, key= lambda x: (x[0] [0],x[1],-x[0][1])))
        # for i in resultnotsorted.take(10) : print(i)

        #coalesce is use to have 2 files in hdfs
        result.coalesce(2).saveAsTextFile("/user/krishnateja/daily_revenue_txt_python")


ps = ProblemStatment()
#ps.PreviewData()
ps.FilterOrders()

