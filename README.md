# BUILD
    sbt clean assembly

# RUN
    ./spark-submit --class DriverName --master local[2] spark-examples.jar
