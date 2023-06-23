ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cake-demo"
MOUNT_FOLDER = "s3_cake"

MOUNT_DIR = "s3a://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME)
dbutils.fs.mount(MOUNT_DIR, f"/mnt/minjiwoo/{MOUNT_FOLDER}")
display(dbutils.fs.ls(f"/mnt/{MOUNT_FOLDER}"))

df = spark.read.format("csv").load(f"dbfs:/mnt/minjiwoo/{MOUNT_FOLDER}/")

articles_df = spark.read.csv("dbfs:/mnt/s3_cake/shared_articles.csv", header=True)
articles_df.show(5)
articles_df.write.saveAsTable("shared_articles")

