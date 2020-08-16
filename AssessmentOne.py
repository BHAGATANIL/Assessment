

from pyspark import SparkConf, SparkContext

#conf = SparkConf().setAppName("read text file in pyspark")
#sc = SparkContext(conf=conf)

import pyspark.sql.functions as f

from pyspark.sql import SparkSession



spark = SparkSession \
    .builder \
    .appName("Courses") \
    .master("local") \
    .getOrCreate()

course = spark.read.csv("C:/Users/lcsuser/Desktop/My Computer/Project/Assessment Data/courses/course.csv",header=True);
enrollment = spark.read.csv("C:/Users/lcsuser/Desktop/My Computer/Project/Assessment Data/courses/enrollment.csv",header=True);
result = spark.read.csv("C:/Users/lcsuser/Desktop/My Computer/Project/Assessment Data/courses/result.csv",header=True);
user = spark.read.csv("C:/Users/lcsuser/Desktop/My Computer/Project/Assessment Data/courses/user.csv",header=True);

#course.show()
#enrollment.show()
#result.show()
#user.show()


course_enroll = course.join(enrollment, course.cid == enrollment.cid,how='outer')

tbl_course_enroll = course_enroll.groupBy(course.cid, course.cname)\
                    .agg(f.max('score').alias('High_score'), f.min('score').alias('Low_score'), f.count('uid').alias('Total_User'))


enrol_user = enrollment.join(user, enrollment.uid == user.uid).join(course, enrollment.cid == course.cid)

tbl_enrol_user = enrol_user.where((enrollment.status == 'active') & (user.status == 'true'))\
                 .groupBy(course.cid, enrollment.uid).count()

final_result = tbl_course_enroll.join(tbl_enrol_user, tbl_course_enroll.cid == tbl_enrol_user.cid,how='outer')\
               .groupBy(tbl_course_enroll.cid, 'cname', 'High_score', 'Low_score', 'Total_user').agg(f.count("uid").alias("Active_user"))

final_result.show()