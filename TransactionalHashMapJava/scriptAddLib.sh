#!/bin/bash

mvn deploy:deploy-file -Durl=file:/Users/gomes/Documents/Projectos/OpenSource/2015/YCSB/TransactionalHashMapJava/repo/ -Dfile=lib/TransactionalHashMapJava.jar -DgroupId=com.example -DartifactId=myhashdb -Dpackaging=jar -Dversion=1.0.1
