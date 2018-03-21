# README #

Recommandation System with Spark Scala and mongoDB

Missing Data with missingno Python

Read data from mongoDB or CSV

Recommandations for totally new user
Top 10 most viewed items
Top 10 most viewed categories
TODO : add top 10 items/categories with geographical precision

Parcours clients -> IDCustomer + [id item bough n°1, category item bough n°1 ........ id item bough n°5, category item bough n°5] + personal infos about customer
Kmeans over customer history to give label
Machine Learning to learn label and classify an incoming user with his history

TODO : implements collaborative associations -> people 1 bought items A & B, people 2 bought items B & C -> recommand C to people 1 and A to people 2 because they got B in common
TODO : implements item to item associations

TODO : cluster mongoDB
