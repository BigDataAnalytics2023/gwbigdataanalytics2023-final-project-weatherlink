val path = "/input/input.csv"
val df3 = spark.read.option("delimiter", ",").option("header", "true").csv(path)
df3.sort("Zipcode").show(true)
df3.orderBy("UnitID").show(true)
df3.write.csv("/input/output2.csv")
df3.coalesce(1)
df3.sort("UnitID").show(true)
df3.coalesce(1).write.mode("overwrite").option("header","true").csv("/input/output6.csv")


